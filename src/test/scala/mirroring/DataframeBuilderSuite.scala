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

import mirroring.builders.{DataframeBuilder, DataframeBuilderContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

class DataframeBuilderSuite extends AnyFunSuite {

  test("renameColumns should take a Dataset and return the same") {
    val spark = SparkSession
      .builder()
      .appName("spark testing")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val df = spark.sparkContext
      .parallelize(
        Seq((1, "2019-10-05", "00", "A"), (2, "2019-10-05", "01", "B"))
      )
      .toDF("id", "date", "hour", "content")
    val result = DataframeBuilder.renameColumns(df)
    assert(
      result.schema.toDDL == "`id` INT NOT NULL,`date` STRING,`hour` STRING,`content` STRING"
    )
  }

  test(
    "renameColumns should take a Dataset and return the same but with special chars replaced in column names"
  ) {
    val spark = SparkSession
      .builder()
      .appName("spark testing")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val df = spark.sparkContext
      .parallelize(
        Seq((1, "2019-10-05", "00", "A"), (2, "2019-10-05", "01", "B"))
      )
      .toDF("id:", "d(at)e", "ho*ur.", "cont ent")
    val result = DataframeBuilder.renameColumns(df)
    assert(
      result.schema.toDDL == "`id__` INT NOT NULL,`date` STRING,`ho*ur__` STRING,`cont__ent` STRING"
    )
  }

  test("buildDataFrame should edit schema") {
    //RUN THIS TEST SEPARATELY
    val spark = SparkSession
      .builder()
      .appName("spark testing")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val df = spark.sparkContext
      .parallelize(
        Seq(
          (1, "2019-10-05", "00", "A", "2022-06-05t00:00:00.000"),
          (2, "2019-10-09", "01", "B", "2022-06-05t00:00:00.000")
        )
      )
      .toDF("i:d", "date", "hour", "content", "ts")
      .withColumn("date", col("date").cast("DATE"))
      .withColumn("ts", col("ts").cast("TIMESTAMP"))
    val context = DataframeBuilderContext(
      targetTableName = "targetTableName",
      writePartitioned = true,
      partitionColumns = Array("ts", "i:d"),
      timezone = Config.Timezone,
      generateColumn = true,
      generatedColumnExp = "day(date)",
      generatedColumnName = "test",
      generatedColumnType = "string"
    )
    val resultDs = DataframeBuilder.buildDataFrame(df, context)
    val expectedSchema = Array(
      ("i__d", "IntegerType"),
      ("date", "TimestampType"),
      ("hour", "StringType"),
      ("content", "StringType"),
      ("ts", "DateType"),
      ("test", "StringType")
    )
    assert(resultDs.dtypes.sameElements(expectedSchema))
    assert(resultDs.select("test").take(1)(0).getString(0).equals("5"))
    assert(resultDs.select("test").take(2)(1).getString(0).equals("9"))
  }
}
