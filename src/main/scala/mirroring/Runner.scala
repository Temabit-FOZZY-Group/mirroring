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

import io.delta.tables.DeltaTable
import mirroring.builders.{ConfigBuilder, DataframeBuilder, FilterBuilder}
import mirroring.config.{Config, FlowLogger, LoggerConfig}
import mirroring.handlers.ChangeTrackingHandler
import mirroring.services.databases.{JdbcCTService, JdbcContext, JdbcPartitionedService, JdbcService}
import mirroring.services.writer.{ChangeTrackingService, DeltaService, MergeService, WriterContext}
import mirroring.services.{DeltaTableService, SparkContextTrait, SqlService}
import org.apache.spark.sql.DataFrame
import wvlet.log.{LogSupport, Logger}

object Runner extends LogSupport with SparkContextTrait {

  def main(args: Array[String]): Unit = {
    // preliminary FlowLogger initialization in order to log config building
    FlowLogger.init()

    val config: Config = initConfig(args)
    logger.info("Starting mirroring-lib...")
    setSparkContext(config)

    val writerContext: WriterContext = config.getWriterContext
    val jdbcDF: DataFrame            = loadDataFromSqlSource(config, writerContext)

    val sqlSourceData = DataframeBuilder.buildDataFrame(jdbcDF, config.getDataframeBuilderContext)

    val writerService = getWriteService(config, writerContext)
    writerService.write(sqlSourceData)

    deltaPostProcessing(config, sqlSourceData, writerService.getUserMetadataJSON)
  }

  def initConfig(args: Array[String]): Config = {
    val config: Config = ConfigBuilder.build(ConfigBuilder.parse(args))
    logger.debug(s"Parameters parsed: ${config.toString}")
    config
  }

  def setSparkContext(config: Config): Unit = {

    val spark = getSparkSession
    spark.sparkContext.setLogLevel(config.logSparkLvl)
    spark.conf.set("spark.sql.session.timeZone", config.timezone)

    logger.info(
      s"""Creating spark session with configurations: ${spark.conf.getAll
        .mkString(", ")}"""
    )

    val loggerConfig = LoggerConfig(
      schema = config.schema,
      table = config.tab,
      logLevel = config.logLvl,
      applicationId = spark.sparkContext.applicationId,
      applicationAttemptId = spark.sparkContext.applicationAttemptId.getOrElse("1")
    )

    FlowLogger.init(loggerConfig)
  }

  private def loadDataFromSqlSource(config: Config, writerContext: WriterContext): DataFrame = {
    val jdbcContext = config.getJdbcContext

    lazy val changeTrackingHandler: ChangeTrackingHandler = new ChangeTrackingHandler(config)
      with SparkContextTrait
    lazy val isDeltaTableExists: Boolean =
      DeltaTable.isDeltaTable(getSparkSession, config.pathToSave)

    def getQuery: String = {
      if (config.isChangeTrackingEnabled) {
        changeTrackingHandler.changeTrackingFlow(isDeltaTableExists, writerContext, jdbcContext)
        changeTrackingHandler.query(isDeltaTableExists)
      } else {
        config.query
      }
    }

    val jdbcDF: DataFrame =
      if (config.isChangeTrackingEnabled && isDeltaTableExists && config.CTChangesQuery.nonEmpty) {
        changeTrackingHandler.changeTrackingFlow(isDeltaTableExists, writerContext, jdbcContext)
        logger.info("Change Tracking: use custom ctChangesQuery")
        JdbcCTService.loadData(jdbcContext)
      } else {
        val jdbcService: JdbcService = getJdbcService(config, jdbcContext)
        jdbcService.loadData(getQuery)
      }
    jdbcDF
  }

  private def getJdbcService(config: Config, jdbcContext: JdbcContext) = {
    if (config.splitBy.nonEmpty) {
      new JdbcPartitionedService(jdbcContext) with SparkContextTrait
    } else {
      new JdbcService(jdbcContext) with SparkContextTrait
    }
  }

  private def getWriteService(config: Config, writerContext: WriterContext) = {
    if (config.isChangeTrackingEnabled) {
      new ChangeTrackingService(writerContext) with SparkContextTrait
    } else if (config.useMerge) {
      new MergeService(writerContext) with SparkContextTrait
    } else {
      new DeltaService(writerContext) with SparkContextTrait
    }
  }

  private def deltaPostProcessing(config: Config, ds: DataFrame, userMetadataJSON: String): Unit = {
    if (config.zorderby_col.nonEmpty) {
      val replaceWhere =
        FilterBuilder.buildReplaceWherePredicate(
          ds = ds,
          partitionCol = config.lastPartitionCol
        )
      DeltaTableService.executeZOrdering(
        config.pathToSave,
        config.zorderby_col,
        replaceWhere
      )
    }
    if (config.hiveDb.nonEmpty) {
      SqlService.run(config, userMetadataJSON)
    }
  }

}
