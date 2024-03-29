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

import mirroring.builders.{ChangeTrackingBuilder, JdbcBuilder}
import mirroring.localrunner.SparkContextTestTrait
import mirroring.services.databases.JdbcContext
import org.scalatest.funsuite.AnyFunSuite

class ChangeTrackingBuilderSuite extends AnyFunSuite {

  test(
    "buildSelect should return statement to retrieve data from CHANGETABLE)"
  ) {
    val PrimaryKeySelectClause: String    = "CT.id as [SYS_CHANGE_PK_id]"
    val schema: String                    = "dbo"
    val sourceTable: String               = "sourceTable"
    val changeTrackingLastVersion: BigInt = 42
    val primaryKeyOnClause                = "T.id = CT.id"
    val ctCurrentVersion: BigInt          = 12345
    val result = ChangeTrackingBuilder.buildSelect(
      primaryKeySelectClause = PrimaryKeySelectClause,
      schema = schema,
      sourceTable = sourceTable,
      changeTrackingLastVersion = changeTrackingLastVersion,
      primaryKeyOnClause = primaryKeyOnClause,
      ctCurrentVersion = ctCurrentVersion
    )
    val query: String =
      s"""(
         |SELECT
         |    T.*, CT.SYS_CHANGE_OPERATION, CT.id as [SYS_CHANGE_PK_id]
         |FROM
         |    [dbo].[sourceTable] AS T WITH (FORCESEEK)
         |RIGHT OUTER JOIN
         |    CHANGETABLE(CHANGES [dbo].[sourceTable], 42) AS CT
         |ON
         |    T.id = CT.id
         |WHERE
         |    CT.SYS_CHANGE_VERSION <= 12345
         |) AS subq
         |""".stripMargin

    assert(result.equals(query))
  }

  test(
    "buildMinValidVersionQuery should return Select to retrieve CHANGE_TRACKING_MIN_VALID_VERSION"
  ) {
    val schema: String = "dbo"
    val tab: String    = "table"

    val result = ChangeTrackingBuilder.buildMinValidVersionQuery(schema, tab)

    assert(
      result.equals(
        """(SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('dbo.table'))
        | as CTver) as subq""".stripMargin
      )
    )
  }

  test(
    "buildPrimaryKeySelectClause should return list of columns with aliases"
  ) {
    val primaryKey: Array[String] = Array("id", "FilId", "id2")
    val result                    = ChangeTrackingBuilder.buildPrimaryKeySelectClause(primaryKey)
    assert(
      result.equals(
        "CT.id as [SYS_CHANGE_PK_id], CT.FilId as [SYS_CHANGE_PK_FilId], CT.id2 as [SYS_CHANGE_PK_id2]"
      )
    )
  }

  test(
    "buildCTQueryParams with default values"
  ) {
    val CTChangesQueryParams: Array[String] =
      Array("defaultSQLTable", "X", "queryCTCurrentVersion", "queryCTLastVersion")
    val jdbcContext: JdbcContext = JdbcContext(
      jdbcUrl = "_jdbcUrl",
      inTable = "tab",
      inSchema = "schema",
      numPart = "numPart",
      splitby = "splitBy",
      _CTChangesQuery = "CTChangesQuery",
      _ctCurrentVersion = Some(BigInt(1)),
      _CTChangesQueryParams = CTChangesQueryParams,
      _changeTrackingLastVersion = () => Some(BigInt(2))
    )

    val jdbcBuilder = new JdbcBuilder with SparkContextTestTrait
    val result      = jdbcBuilder.buildCTQueryParams(CTChangesQueryParams, jdbcContext)
    assert(result sameElements Array("[schema].[tab]", "X", "1", "2"))
  }
}
