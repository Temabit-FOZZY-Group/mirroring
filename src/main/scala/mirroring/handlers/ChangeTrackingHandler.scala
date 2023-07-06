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

package mirroring.handlers

import io.delta.tables.DeltaTable
import mirroring.builders._
import mirroring.config.{Config, UserMetadata}
import mirroring.services.SparkContextTrait
import mirroring.services.databases.{JdbcCTService, JdbcContext}
import mirroring.services.writer.WriterContext
import wvlet.airframe.codec.MessageCodec
import wvlet.log.LogSupport

class ChangeTrackingHandler(
    config: Config,
    jdbcCTService: JdbcCTService
) extends LogSupport {
  this: SparkContextTrait =>

  lazy val ctCurrentVersion: BigInt = {
    logger.info(s"Querying current change tracking version from the source...")
    val version: BigInt = if (config.CTCurrentVersionQuery.isEmpty) {
      logger.info("Change Tracking: use default query to get CTCurrentVersion")
      jdbcCTService.getChangeTrackingVersion(
        query = ChangeTrackingBuilder.currentVersionQuery
      )
    } else {
      logger.info("Change Tracking: use custom CTCurrentVersionQuery")
      jdbcCTService.getChangeTrackingVersion(
        query = config.CTCurrentVersionQuery,
        parameters = config.CTCurrentVersionParams
      )
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
      jdbcCTService.getChangeTrackingVersion(
        query = ChangeTrackingBuilder.buildMinValidVersionQuery(config.schema, config.tab)
      )
    } else {
      logger.info("Change Tracking: use custom CTMinValidVersionQuery")
      jdbcCTService.getChangeTrackingVersion(
        query = config.CTMinValidVersionQuery,
        parameters = config.CTMinValidVersionParams
      )
    }
    logger.info(s"Min valid version for the MSSQL table: $version")
    version
  }

  private lazy val ctDeltaVersion: BigInt = {
    val userMetaJSON = DeltaTable
      .forPath(getSparkSession, config.pathToSave)
      .history()
      .where("operation <> 'OPTIMIZE'")
      .select("userMetadata")
      .collect()(0)
      .getString(0)

    val codec = MessageCodec.of[UserMetadata]
    // TO DO: remove condition in v1.2.0. For backward comp. when "userMetadata" is a number
    if (userMetaJSON.forall(Character.isDigit)) {
      BigInt(userMetaJSON)
    } else {
      val userMetadata: Option[UserMetadata] = codec.unpackJson(userMetaJSON)
      val ChangeTrackingVersion: BigInt      = userMetadata.get.ChangeTrackingVersion
      ChangeTrackingVersion
    }
  }

  private lazy val ctAppendDeltaVersion: BigInt = {
    val userMetaJSON = DeltaTable
      .forPath(getSparkSession, config.pathToSave)
      .history()
      .where("operation <> 'OPTIMIZE'")
      .select("userMetadata")
      .collect()

    val codec = MessageCodec.of[UserMetadata]
    val userMetadata: Array[UserMetadata] =
      userMetaJSON.map(_.getString(0)).flatMap { JSON =>
        if (JSON == null) None else codec.unpackJson(JSON)
      }
    val ChangeTrackingVersion: BigInt = userMetadata.map(_.ChangeTrackingVersion).max
    ChangeTrackingVersion
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
    changeTrackingLastVersion = changeTrackingLastVersion(false),
    primaryKeyOnClause = primaryKeyOnClause,
    ctCurrentVersion = ctCurrentVersion
  )

  private lazy val ctChangesAppendQuery: String = ChangeTrackingBuilder.buildSelectWithVersion(
    primaryKeySelectClause = primaryKeySelectClause,
    schema = config.schema,
    sourceTable = config.tab,
    changeTrackingLastVersion = changeTrackingLastVersion(true),
    primaryKeyOnClause = primaryKeyOnClause,
    ctCurrentVersion = ctCurrentVersion
  )

  def changeTrackingLastVersion(isCtAppendModeEnabled: Boolean): BigInt = {
    var changeTrackingLastVersion: BigInt = -1
    try {
      changeTrackingLastVersion =
        if (isCtAppendModeEnabled) ctAppendDeltaVersion else ctDeltaVersion
      logger.info(
        s"Last change tracking version extracted from the delta table: ${changeTrackingLastVersion.toString}"
      )
      changeTrackingLastVersion
    } catch {
      case _ @(_: java.lang.NumberFormatException | _: java.lang.NullPointerException) =>
        logger.warn(
          "No CT version found in the latest version of the userMetadata. CHANGE_TRACKING_MIN_VALID_VERSION will be used."
        )
        ctMinValidVersion
      case e: org.apache.spark.sql.AnalysisException
          if e.getMessage.endsWith("is not a Delta table.") =>
        logger.warn(
          s"${e.getMessage} CHANGE_TRACKING_MIN_VALID_VERSION will be used."
        )
        ctMinValidVersion
      case e: java.lang.UnsupportedOperationException =>
        logger.warn(
          s"${e.getMessage} CHANGE_TRACKING_MIN_VALID_VERSION will be used."
        )
        ctMinValidVersion
    }
  }

  def query(isDeltaTableExists: Boolean, ctAppendMode: Boolean): String = {
    if (isDeltaTableExists || ctAppendMode) {
      getCtChangesQuery(ctAppendMode)
    } else {
      logger.info("Target table doesn't exist yet. Reading data in full ...")
      s"(select * from [${config.schema}].[${config.tab}] with (nolock) where 1=1) as subq"
    }
  }

  def changeTrackingFlow(
      isDeltaTableExists: Boolean,
      currentWriterContext: WriterContext,
      currentJdbcContext: JdbcContext
  ): Unit = {
    currentWriterContext._ctCurrentVersion = Some(ctCurrentVersion)
    currentJdbcContext._ctCurrentVersion = Some(ctCurrentVersion)
    currentJdbcContext._changeTrackingLastVersion = () =>
      Some(changeTrackingLastVersion(currentWriterContext.isCtAppendModeEnabled))
    if (isDeltaTableExists) {
      require(
        changeTrackingLastVersion(currentWriterContext.isCtAppendModeEnabled) >= ctMinValidVersion,
        "Invalid Change Tracking version. Client table must be reinitialized."
      )
    }
  }

  private def getCtChangesQuery(ctAppendMode: Boolean): String = {
    if (ctAppendMode) ctChangesAppendQuery else ctChangesQuery
  }

}
