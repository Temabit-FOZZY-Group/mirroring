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

package mirroring.builders

import wvlet.log.LogSupport

import java.time.LocalDate

object SqlBuilder extends LogSupport {

  def buildSQLObjectName(
      schema: String,
      table: String
  ): String = {
    s"[$schema].[$table]"
  }

  def buildSelectTableSQL(
      schema: String,
      table: String,
      where: String
  ): String = {
    s"(select * from [$schema].[$table] with (nolock) where $where) as subq"
  }

  def buildBetween(
      dtFlt: String,
      startDate: LocalDate,
      endDate: LocalDate
  ): String = {
    s" AND $dtFlt >= '${startDate}T00:00:00.000' AND $dtFlt < '${endDate}T00:00:00.000'"
  }

  def buildDropTableSQL(db: String, tab: String): String = {
    s"DROP TABLE IF EXISTS `$db`.`$tab`;"
  }

  def buildCreateTableSQL(
      db: String,
      tab: String,
      dataPath: String,
      logRetentionDuration: String,
      deletedFileRetentionDuration: String
  ): String = {
    s"""CREATE EXTERNAL TABLE `$db`.`$tab`
    |USING DELTA
    |LOCATION '$dataPath'
    |TBLPROPERTIES (
    |   'delta.logRetentionDuration' = '$logRetentionDuration',
    |   'delta.deletedFileRetentionDuration' = '$deletedFileRetentionDuration'
    |   );
    """.stripMargin
  }

  def buildCreateDbSQL(db: String, dbLocation: String): String = {
    s"CREATE DATABASE IF NOT EXISTS `$db` LOCATION '$dbLocation';"
  }

}
