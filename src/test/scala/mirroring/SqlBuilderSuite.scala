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

import mirroring.builders.SqlBuilder
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate

class SqlBuilderSuite extends AnyFunSuite {
  test("buildSelectTableSQL should produce select statement") {
    val table  = "TestTable"
    val schema = "dbo"
    val where  = "test=1"
    val result = SqlBuilder.buildSelectTableSQL(schema, table, where)
    assert(
      result == "(select * from [dbo].[TestTable] with (nolock) where test=1) as subq"
    )
  }

  test("buildBetween should generate filter for dates") {
    val dtFlt  = "test=1"
    val start  = LocalDate.parse("2022-01-20")
    val month  = LocalDate.parse("2022-03-27")
    val result = SqlBuilder.buildBetween(dtFlt, start, month)
    assert(
      result == " AND test=1 >= '2022-01-20T00:00:00.000' AND test=1 < '2022-03-27T00:00:00.000'"
    )
  }

  test("buildDropTableSQL should create DROP TABLE statement") {
    val table  = "Test1Table"
    val db     = "default"
    val result = SqlBuilder.buildDropTableSQL(db, table)
    assert(result == "DROP TABLE IF EXISTS `default`.`Test1Table`;")
  }

  test(
    "buildCreateTableSQL should generate CREATE EXTERNAL TABLE statement"
  ) {
    val table    = "Test1Table"
    val db       = "default"
    val dataPath = "s3a://bucket/folder"
    val result = SqlBuilder.buildCreateTableSQL(
      db,
      table,
      dataPath
    )
    val expectedResult =
      s"""CREATE EXTERNAL TABLE `default`.`Test1Table`
       |USING DELTA
       |LOCATION 's3a://bucket/folder';
    """.stripMargin
    assert(result == expectedResult)
  }

  test(
    "buildAlterTableSQL should generate ALTER TABLE statement"
  ) {
    val table                        = "Test1Table"
    val db                           = "default"
    val logRetentionDuration         = "interval 1 day"
    val deletedFileRetentionDuration = "interval 1 day"
    val result = SqlBuilder.buildAlterTableSQL(
      db,
      table,
      logRetentionDuration,
      deletedFileRetentionDuration
    )
    val expectedResult =
      s"""ALTER TABLE `default`.`Test1Table`
         |SET TBLPROPERTIES (
         |'delta.logRetentionDuration'= 'interval 1 day',
         |'delta.deletedFileRetentionDuration'= 'interval 1 day'
         |);""".stripMargin

    assert(result == expectedResult)
  }

  test("buildCreateDbSQL should return CREATE DATABASE statement ") {
    val db       = "default"
    val location = "s3a://bucket/folder"
    val result   = SqlBuilder.buildCreateDbSQL(db, location)
    assert(
      result == "CREATE DATABASE IF NOT EXISTS `default` LOCATION 's3a://bucket/folder';"
    )
  }

}
