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
package mirroring.localrunner

import org.json4s.{DefaultFormats, Extraction}

case class TestConfig(
    jdbcUrl: String,
    schema: String,
    sourceTable: String,
    pathToSave: String,
    whereClause: String,
    query: String,
    changeTrackingEnabled: String,
    primaryKey: String
)

object TestConfig {

  val argumentAliasMap: Map[String, String] = Map(
    "jdbcUrl"               -> "jdbcUrl",
    "schema"                -> "schema",
    "sourceTable"           -> "tab",
    "pathToSave"            -> "path_to_save",
    "whereClause"           -> "where",
    "query"                 -> "query",
    "changeTrackingEnabled" -> "change_tracking",
    "primaryKey"            -> "primary_key"
  )
  implicit class EnrichedTestConfig(testConfig: TestConfig) {
    def getApplicationArguments: Array[String] = {
      val configMap = Extraction
        .decompose(testConfig)(DefaultFormats)
        .values
        .asInstanceOf[Map[String, String]]

      val args = configMap
        .filter(_._2.nonEmpty)
        .map { case (fieldName, fieldValue) => s"${getArgumentAlias(fieldName)}==$fieldValue" }
        .toArray

      args
    }

    def getArgumentAlias(fieldName: String): String = {
      argumentAliasMap(fieldName)
    }
  }
}
