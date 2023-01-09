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

import mirroring.builders.ConfigBuilder
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

trait SparkSessionLender {
  def withLocalSparkContext(testFunction: SparkSession => Any): Unit
}

class ConfigBuilderSuite extends AnyFunSuite with SparkSessionLender {
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

  test(
    "parse invocation should return a Map[String, String] with parsed program arguments"
  ) {
    val args: Array[String] = Array(
      "tab==ChequeLineActions",
      "schema==dbo",
      "splitby==CREATED",
      "numpart==5",
      "calc_min_dt==None",
      "calc_max_dt==None",
      "dtflt==CREATED",
      "exec_date==2022-03-29",
      "log_lvl==warn",
      "log_spark_lvl==WARN",
      "write_partitioned==True",
      "partition_col==id",
      "use_merge==True",
      "merge_keys==id",
      "hive_db==test"
    )
    val config = ConfigBuilder.parse(args)
    assert(config("tab") == "ChequeLineActions")
    assert(config("schema") == "dbo")
    assert(config("splitby") == "CREATED")
    assert(config("numpart") == "5")
    assert(config("calc_min_dt") == "")
    assert(config("calc_max_dt") == "")
    assert(config("dtflt") == "CREATED")
    assert(config("log_lvl") == "warn")
    assert(config("log_spark_lvl") == "WARN")
    assert(config("use_merge") == "True")
    assert(config("merge_keys") == "id")
  }

//  test("build should build config with correct parameters") {
//    withLocalSparkContext(spark => {
//      val args: Array[String] = Array(
//        "path_to_save==/user/test/anothercopy/",
//        "jdbcUrl==jdbc_path_db;user=test;password=test;",
//        "tab==ChequeLineActions",
//        "schema==dbo",
//        "splitby==CREATED",
//        "numpart==5",
//        "calc_min_dt==None",
//        "calc_max_dt==None",
//        "dtflt==CREATED",
//        "exec_date==2022-03-29",
//        "write_partitioned==True",
//        "partition_col==test",
//        "dtflt==test",
//        "use_merge==False",
//        "merge_keys==None",
//        "hive_db==test"
//      )
//      val config = ConfigBuilder.build(ConfigBuilder.parse(args))
//      assert(config.targetTableName == "dbo__chequelineactions")
//      assert(config.minDate.toString == "2022-03-29")
//      assert(config.maxDate.toString == "2022-03-30")
//      assert(
//        config.pathToSave == "/user/test/anothercopy//dbo__chequelineactions"
//      )
//      assert(config.partitionCols === Array("test"))
//      assert(config.logLvl == "info")
//      assert(config.logSparkLvl == "WARN")
//    })
//  }

//  test("build should build config with correct parameters 2") {
//    withLocalSparkContext(spark => {
//      val args: Array[String] = Array(
//        "tab==tab_holesdo_mainalllagers",
//        "schema==dbo",
//        "hive_db==user",
//        "dtflt==date",
//        "path_to_save==/user/test/anothercopy/",
//        "calc_min_dt==2022-03-15",
//        "calc_max_dt==2022-03-19",
//        "jdbcUrl==jdbc:jtds:sqlserver://KVCEN15-SQLS005.officekiev.fozzy.lan;instance=heavy005;domain=officekiev.fozzy.lan;useNTLMv2=true;databasename=ella;user=x;password=y;",
//        "mode==overwrite",
//        "write_partitioned==True",
//        "partition_col==businessId, date",
//        "log_lvl==info"
//      )
//      val config = ConfigBuilder.build(ConfigBuilder.parse(args))
//      assert(config.targetTableName == "dbo__tab_holesdo_mainalllagers")
//      assert(config.minDate.toString == "2022-03-15")
//      assert(config.maxDate.toString == "2022-03-19")
//      assert(
//        config.pathToSave == "/user/test/anothercopy//dbo__tab_holesdo_mainalllagers"
//      )
//      assert(config.partitionCols === Array("businessId", "date"))
//      assert(config.logLvl == "info")
//      assert(config.logSparkLvl == "WARN")
//    })
//  }
}
