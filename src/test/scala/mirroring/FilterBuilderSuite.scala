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

package mirroring

import mirroring.builders.{ConfigBuilder, FilterBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.funsuite.AnyFunSuite

class FilterBuilderSuite extends AnyFunSuite {

  test("buildJoinCondition should return String with join clause") {
    val primaryKey: Array[String] = Array("id", "FilId", "id2")
    val result                    = FilterBuilder.buildJoinCondition(primaryKey, "T", "CT")
    assert(
      result.equals("T.id = CT.id and T.FilId = CT.FilId and T.id2 = CT.id2")
    )
  }

  test("buildStrWithoutSpecChars should replace special chars in input") {
    val input  = "test_some*chars&."
    val result = FilterBuilder.buildStrWithoutSpecChars(input)
    assert(result.equals("test_some_chars__"))
  }

  test("buildStrWithoutSpecChars should replace special chars in input with __") {
    val input  = "test_some*chars&."
    val result = FilterBuilder.buildStrWithoutSpecChars(input, "__")
    assert(result.equals("test_some__chars____"))
  }

  test(
    "buildReplaceWherePredicate should collect values from dataset to replace them on target"
  ) {
    val spark = SparkSession
      .builder()
      .appName("spark testing")
      .master("local")
      .getOrCreate()
    val partitionCol = "id"
    import spark.implicits._
    val df = spark.sparkContext
      .parallelize(
        Seq((1, "2019-10-05", "00", "A"), (2, "2019-10-05", "01", "B"))
      )
      .toDF("id", "date", "hour", "content")
    val result = FilterBuilder.buildReplaceWherePredicate(df, partitionCol)
    assert(result == "id in (1, 2)")
  }

  test("buildReplaceWherePredicate with empty partition") {
    val spark = SparkSession
      .builder()
      .appName("spark testing")
      .master("local")
      .getOrCreate()
    val partitionCol = ""
    import spark.implicits._
    val df = spark.sparkContext
      .parallelize(
        Seq((1, "2019-10-05", "00", "A"), (2, "2019-10-05", "01", "B"))
      )
      .toDF("id", "date", "hour", "content")
    val result = FilterBuilder.buildReplaceWherePredicate(df, partitionCol)
    assert(result == "")
  }

  test(
    "buildReplaceWherePredicate with empty partition and null dataframe"
  ) {
    val partitionCol = ""
    val df           = null
    val result       = FilterBuilder.buildReplaceWherePredicate(df, partitionCol)
    assert(result == "")
  }

  test(
    "buildReplaceWherePredicate should collect values from dataset to replace them on target and append where clause from user"
  ) {
    val partitionCol             = "id"
    val partitionColTargetSchema = "target"
    val whereClause =
      "(filid = 3171 and cast(operationdate as date) = '2022-12-02')"
    val spark = SparkSession
      .builder()
      .appName("spark testing")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val df = spark.sparkContext
      .parallelize(
        Seq((1, "2019-10-05", "00", "A"), (2, "2019-10-05", "01", "B"))
      )
      .toDF("id", "date", "hour", "content")
    val result = FilterBuilder.buildReplaceWherePredicate(
      ds = df,
      partitionCol = partitionCol,
      partitionColTargetSchema = partitionColTargetSchema,
      whereClause = whereClause
    )
    assert(
      result == "(filid = 3171 and cast(operationdate as date) = '2022-12-02') AND target.id in (1, 2)"
    )
  }

  test(
    "buildReplaceWherePredicate with empty partition and null dataframe and where clause from user should return where clause"
  ) {
    val partitionCol = ""
    val df           = null
    val whereClause  = "(filid = 3171 and cast(operationdate as date) = '2022-12-02')"
    val result = FilterBuilder.buildReplaceWherePredicate(
      ds = df,
      partitionCol = partitionCol,
      whereClause = whereClause
    )
    assert(result == "(filid = 3171 and cast(operationdate as date) = '2022-12-02')")
  }

  test("buildMergeCondition should return String with condition") {
    val args: Array[String] = Array(
      "path_to_save==dummy",
      "jdbcUrl==dummy;user=hhh;password=ll",
      "tab==dummy",
      "change_tracking==True",
      "primary_key==id,[FilId], id2"
    )
    val config = ConfigBuilder.build(ConfigBuilder.parse(args))
    val result = FilterBuilder.buildMergeCondition(
      primaryKey = config.primary_key,
      sourceColPrefix = "SYS_CHANGE_PK_",
      ds = null
    )
    assert(
      result.equals(
        "source.SYS_CHANGE_PK_id = target.id and source.SYS_CHANGE_PK_FilId = target.FilId " +
          "and source.SYS_CHANGE_PK_id2 = target.id2"
      )
    )
  }

  test("buildMergeCondition should return String with condition 2") {
    val spark = SparkSession
      .builder()
      .appName("spark testing")
      .master("local")
      .getOrCreate()
    val args: Array[String] = Array(
      "path_to_save==dummy",
      "jdbcUrl==dummy;user=hhh;password=ll",
      "tab==dummy",
      "change_tracking==true",
      "primary_key==id,[FilId], id2",
      "write_partitioned==true",
      "partition_col==FilId"
    )
    val config = ConfigBuilder.build(ConfigBuilder.parse(args))
    val ds = spark
      .range(3)
      .withColumn("FilId", col("id").multiply(10))
      .withColumn("id2", lit(0))
      .withColumn("SYS_CHANGE_PK_FilId", lit(1))
    val result = FilterBuilder.buildMergeCondition(
      primaryKey = config.primary_key,
      partitionCol = "FilId",
      ds = ds,
      partitionColPrefix = "SYS_CHANGE_PK_"
    )
    val expectedResult =
      "target.FilId in (1) and source.id = target.id and source.FilId = target.FilId " +
        "and source.id2 = target.id2"
    assert(result.equals(expectedResult))
  }

  test("buildMergeCondition should return String with condition 3") {
    val spark = SparkSession
      .builder()
      .appName("spark testing")
      .master("local")
      .getOrCreate()
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
      primaryKey = config.primary_key,
      partitionCol = "FilId",
      ds = ds,
      sourceColPrefix = "pref_",
      targetColPrefix = "anotherPref_"
    )
    val expectedResult =
      "target.FilId in (0, 10, 20) and source.pref_id = target.anotherPref_id and " +
        "source.pref_FilId = target.anotherPref_FilId and source.pref_id2 = target.anotherPref_id2"
    assert(result.equals(expectedResult))
  }

  test("buildMergeCondition should return String with condition 4") {
    val args: Array[String] = Array(
      "path_to_save==dummy",
      "jdbcUrl==dummy;user=hhh;password=ll",
      "tab==dummy",
      "change_tracking==true",
      "primary_key==id,[FilId], id2"
    )
    val config = ConfigBuilder.build(ConfigBuilder.parse(args))
    val result = FilterBuilder.buildMergeCondition(primaryKey = config.primary_key, ds = null)
    assert(
      result.equals(
        "source.id = target.id and source.FilId = target.FilId " +
          "and source.id2 = target.id2"
      )
    )
  }
}
