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

object ChangeTrackingBuilder {

  val currentVersionQuery =
    "(SELECT CHANGE_TRACKING_CURRENT_VERSION() as CTver) as subq"

  def buildSelect(
      primaryKeySelectClause: String,
      schema: String,
      sourceTable: String,
      changeTrackingLastVersion: BigInt,
      primaryKeyOnClause: String,
      ctCurrentVersion: BigInt
  ): String = {
    s"""(
       |SELECT
       |    T.*, CT.SYS_CHANGE_OPERATION, $primaryKeySelectClause
       |FROM
       |    [$schema].[$sourceTable] AS T WITH (FORCESEEK)
       |RIGHT OUTER JOIN
       |    CHANGETABLE(CHANGES [$schema].[$sourceTable], $changeTrackingLastVersion) AS CT
       |ON
       |    $primaryKeyOnClause
       |WHERE
       |    CT.SYS_CHANGE_VERSION <= $ctCurrentVersion
       |) AS subq
       |""".stripMargin
  }

  def buildMinValidVersionQuery(schema: String, sourceTable: String): String = {
    s"""(SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('$schema.$sourceTable'))
       | as CTver) as subq""".stripMargin
  }

  def buildPrimaryKeySelectClause(primaryKey: Array[String]): String = {
    primaryKey
      .map(colName => s"CT.${colName.trim} as [SYS_CHANGE_PK_${colName.trim}]")
      .mkString(", ")
  }

}
