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

package palefat.data.mirror.handlers

import io.delta.tables.DeltaTable
import java.sql.DriverManager
import org.apache.spark.sql.DataFrame
import palefat.data.mirror.Config
import palefat.data.mirror.builders._
import palefat.data.mirror.services.SparkService.spark
import palefat.data.mirror.services.databases.{DbService, JdbcService}
import wvlet.log.LogSupport

class ChangeTrackingHandler(config: Config) extends LogSupport {

  private val jdbcContext                  = config.getJdbcContext
  private val jdbcService: DbService       = new JdbcService(jdbcContext)

  private def getChangeTrackingVersionCustom(jdbcUrl: String,
                                             query: String,
                                             parameters: Array[String] = Array[String](),
                                            ): BigInt = {
    val connection = DriverManager.getConnection(jdbcUrl)
    try {
      val rs = JdbcBuilder.buildJDBCResultSet(
        connection,
        query,
        parameters
      )
      rs.next()
      rs.getLong(1)
    } catch {
      case e: Exception => throw e
    } finally {
      connection.close()
    }
  }

  lazy val ctCurrentVersion: BigInt = {
    logger.info(s"Querying current change tracking version from the source...")
    val version: BigInt = if (config.CTCurrentVersionQuery.isEmpty) {
      logger.info("Change Tracking: use default query to get CTCurrentVersion")
      getChangeTrackingVersionDefault(ChangeTrackingBuilder.currentVersionQuery, config)
    } else {
      logger.info("Change Tracking: use custom CTCurrentVersionQuery")
      getChangeTrackingVersionCustom(jdbcService.url, config.CTCurrentVersionQuery)
    }
    logger.info(s"Current CT version for the MSSQL table: $version")
    version
  }

  private lazy val ctMinValidVersion: BigInt = {
    logger.info(
      s"Querying minimum valid change tracking version from the source..."
    )
    val version: BigInt = if (config.CTMinValidVersionQuery.isEmpty) {
      logger.info("Change Tracking: use default query to get ChangeTrackingMinValidVersion")
      getChangeTrackingVersionDefault(ChangeTrackingBuilder.buildMinValidVersionQuery(config.schema, config.tab), config)
    } else {
      logger.info("Change Tracking: use custom CTMinValidVersionQuery")
      val tab: Array[String] = Array(SqlBuilder.buildSQLObjectName(config.schema, config.tab))
      getChangeTrackingVersionCustom(jdbcService.url, config.CTMinValidVersionQuery, tab)
    }
    logger.info(s"Min valid version for the MSSQL table: $version")
    version
  }

  private lazy val ctDeltaVersion: BigInt = {
    val userMeta = DeltaTable
      .forPath(spark, config.pathToSave)
      .history(1)
      .select("userMetadata")
      .collect()(0)
      .getString(0)

    BigInt(userMeta)

  }

  private lazy val primaryKeyOnClause: String = {
    FilterBuilder.buildJoinCondition(config.primary_key, "T", "CT")
  }

  private lazy val primaryKeySelectClause: String = {
    ChangeTrackingBuilder.buildPrimaryKeySelectClause(config.primary_key)
  }

  private lazy val ctChangesQuery: String = ChangeTrackingBuilder.buildSelect(
    primaryKeySelectClause = primaryKeySelectClause,
    schema = config.schema,
    sourceTable = config.tab,
    changeTrackingLastVersion = changeTrackingLastVersion,
    primaryKeyOnClause = primaryKeyOnClause,
    ctCurrentVersion = ctCurrentVersion
  )

  private lazy val changeTrackingLastVersion: BigInt = {
    var changeTrackingLastVersion: BigInt = -1
    try {
      changeTrackingLastVersion = ctDeltaVersion
      logger.info(
        s"Last change tracking version extracted from the delta table: ${changeTrackingLastVersion.toString}"
      )
      changeTrackingLastVersion
    } catch {
      case e @ (_: java.lang.NumberFormatException | _: java.lang.NullPointerException) =>
        logger.warn(
          "No CT version found in the latest version of the userMetadata. CHANGE_TRACKING_MIN_VALID_VERSION will be used."
        )
        changeTrackingLastVersion = ctMinValidVersion
        changeTrackingLastVersion
    }
  }

  def getChangeTrackingVersionDefault(query: String, config: Config): BigInt = {

    val jdbcContext            = config.getJdbcContext
    val jdbcService: DbService = new JdbcService(jdbcContext)
    val jdbcDF: DataFrame      = jdbcService.loadData(query).cache()

    var version: BigInt = BigInt(0)

    if (!jdbcDF.isEmpty) {
      version = BigInt(
        jdbcDF
          .collect()(0)
          .getLong(0)
      )
    }

    version
  }

  def query: String = {
    var query =
      s"(select * from [${config.schema}].[${config.tab}] with (nolock) where 1=1) as subq"
    if (DeltaTable.isDeltaTable(spark, config.pathToSave)) {
      require(
        changeTrackingLastVersion >= ctMinValidVersion,
        "Invalid Change Tracking version. Client table must be reinitialized."
      )
      query = ctChangesQuery
    } else {
      logger.info("Target table doesn't exist yet. Reading data in full ...")
    }
    query
  }

  def loadChangeTrackingChanges(): DataFrame = {
    logger.info("Change Tracking: use custom ctChangesQuery")
    val connection = DriverManager.getConnection(jdbcService.url)
    try {
      val params: Array[String] = JdbcBuilder.buildCTChangesQueryParams(
        config.CTChangesQueryParams,
        config.schema,
        config.tab,
        changeTrackingLastVersion.toString(),
        ctCurrentVersion.toString(),
      )
      val jdbcDF: DataFrame = JdbcBuilder.buildDataFrameFromResultSet(
        JdbcBuilder.buildJDBCResultSet(
          connection,
          config.CTChangesQuery,
          params
        )
      )
      logger.info(s"Number of incoming rows: ${jdbcDF.count}")
      jdbcDF
    } catch {
      case e: Exception => throw e
    } finally {
      connection.close()
    }
  }
}
