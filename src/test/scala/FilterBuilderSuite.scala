/*
 * Copyright (2021) The Delta Flow Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.funsuite.AnyFunSuite
import palefat.data.mirror.builders.{ConfigBuilder, FilterBuilder}

class FilterBuilderSuite extends AnyFunSuite with SparkSessionLender {

  def withLocalSparkContext(testFunction: SparkSession => Any): Unit = {
    val spark = SparkSession
      .builder()
      .appName("spark testing")
      .master("local")
      .getOrCreate()
    try {
      testFunction(spark)
    } finally spark.stop()
  }

  test("buildJoinCondition should return String with join clause") {
    val primaryKey: Array[String] = Array("id", "FilId", "id2")
    val result = FilterBuilder.buildJoinCondition(primaryKey, "T", "CT")
    assert(
      result.equals("T.id = CT.id and T.FilId = CT.FilId and T.id2 = CT.id2")
    )
  }

  test("buildStrWithoutSpecChars should replace special chars in input") {
    val input = "test_some*chars&."
    val result = FilterBuilder.buildStrWithoutSpecChars(input)
    assert(result.equals("test_some_chars__"))
  }

  test(
    "buildStrWithoutSpecChars should replace special chars in input with __"
  ) {
    val input = "test_some*chars&."
    val result = FilterBuilder.buildStrWithoutSpecChars(input, "__")
    println(result)
    assert(result.equals("test_some__chars____"))
  }

  test(
    "buildReplaceWherePredicate should collect values from dataset to replace them on target"
  ) {
    withLocalSparkContext(spark => {
      val partitionCol = "id"
      import spark.implicits._
      val df = spark.sparkContext
        .parallelize(
          Seq((1, "2019-10-05", "00", "A"), (2, "2019-10-05", "01", "B"))
        )
        .toDF("id", "date", "hour", "content")
      val result = FilterBuilder.buildReplaceWherePredicate(df, partitionCol)
      assert(result == "id in (1, 2)")
    })
  }

  test("should BuilderFilter buildReplaceWherePredicate with empty partition") {
    withLocalSparkContext(spark => {
      val partitionCol = ""
      import spark.implicits._
      val df = spark.sparkContext
        .parallelize(
          Seq((1, "2019-10-05", "00", "A"), (2, "2019-10-05", "01", "B"))
        )
        .toDF("id", "date", "hour", "content")
      val result = FilterBuilder.buildReplaceWherePredicate(df, partitionCol)
      assert(result == "")
    })
  }

  test(
    "should BuilderFilter buildReplaceWherePredicate with empty partition and null dataframe"
  ) {
    withLocalSparkContext(spark => {
      val partitionCol = ""
      val df = null
      val result = FilterBuilder.buildReplaceWherePredicate(df, partitionCol)
      assert(result == "")
    })
  }

  test("buildMergeCondition should return String with condition") {
    withLocalSparkContext(spark => {
      val args: Array[String] = Array(
        "path_to_save==dummy",
        "jdbcUrl==dummy;user=hhh;password=ll",
        "tab==dummy",
        "change_tracking==True",
        "primary_key==id,[FilId], id2"
      )
      val config = ConfigBuilder.build(ConfigBuilder.parse(args))
      val result = FilterBuilder.buildMergeCondition(
        config.primary_key,
        sourceColPrefix = "SYS_CHANGE_PK_",
        ds = null
      )
      assert(
        result.equals(
          "source.SYS_CHANGE_PK_id = target.id and source.SYS_CHANGE_PK_FilId = target.FilId " +
            "and source.SYS_CHANGE_PK_id2 = target.id2"
        )
      )
    })
  }

  test("buildMergeCondition should return String with condition 2") {
    withLocalSparkContext(spark => {
      val args: Array[String] = Array(
        "path_to_save==dummy",
        "jdbcUrl==dummy;user=hhh;password=ll",
        "tab==dummy",
        "change_tracking==true",
        "primary_key==id,[FilId], id2"
      )
      val config = ConfigBuilder.build(ConfigBuilder.parse(args))
      val ds = spark
        .range(3)
        .withColumn("FilId", col("id").multiply(10))
        .withColumn("id2", lit(0))
      val result = FilterBuilder.buildMergeCondition(
        config.primary_key,
        partitionCol = "FilId",
        ds = ds
      )
      val expectedResult =
        "target.FilId in (0, 10, 20) and source.id = target.id and source.FilId = target.FilId " +
          "and source.id2 = target.id2"
      assert(result.equals(expectedResult))
    })
  }

  test("buildMergeCondition should return String with condition 3") {
    withLocalSparkContext(spark => {
      val args: Array[String] = Array(
        "path_to_save==dummy",
        "jdbcUrl==dummy;user=hhh;password=ll",
        "tab==dummy",
        "change_tracking==true",
        "primary_key==id,[FilId], id2"
      )
      val config = ConfigBuilder.build(ConfigBuilder.parse(args))
      val ds = spark
        .range(3)
        .withColumn("FilId", col("id").multiply(10))
        .withColumn("id2", lit(0))
      val result = FilterBuilder.buildMergeCondition(
        config.primary_key,
        partitionCol = "FilId",
        ds = ds,
        sourceColPrefix = "pref_",
        targetColPrefix = "anotherPref_"
      )
      val expectedResult =
        "target.FilId in (0, 10, 20) and source.pref_id = target.anotherPref_id and " +
          "source.pref_FilId = target.anotherPref_FilId and source.pref_id2 = target.anotherPref_id2"
      assert(result.equals(expectedResult))
    })
  }

  test("buildMergeCondition should return String with condition 4") {
    withLocalSparkContext(spark => {
      val args: Array[String] = Array(
        "path_to_save==dummy",
        "jdbcUrl==dummy;user=hhh;password=ll",
        "tab==dummy",
        "change_tracking==true",
        "primary_key==id,[FilId], id2"
      )
      val config = ConfigBuilder.build(ConfigBuilder.parse(args))
      val result =
        FilterBuilder.buildMergeCondition(config.primary_key, ds = null)
      assert(
        result.equals(
          "source.id = target.id and source.FilId = target.FilId " +
            "and source.id2 = target.id2"
        )
      )
    })
  }
}
