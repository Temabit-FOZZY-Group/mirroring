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
package mirroring.services
import io.delta.tables.DeltaTable
import mirroring.builders.{DataframeBuilder, FilterBuilder, JdbcBuilder}
import mirroring.config.{Config, FlowLogger, LoggerConfig}
import mirroring.handlers.ChangeTrackingHandler
import mirroring.services.databases.{
  JdbcCTService,
  JdbcContext,
  JdbcPartitionedService,
  JdbcService
}
import mirroring.services.writer.{ChangeTrackingService, DeltaService, MergeService, WriterContext}
import org.apache.spark.sql.DataFrame
import wvlet.log.LogSupport

class MirroringManager extends LogSupport {
  this: SparkContextTrait =>

  def startDataMirroring(config: Config): Unit = {

    setSparkContext(config)

    val writerContext: WriterContext = config.getWriterContext
    val jdbcDF: DataFrame            = loadDataFromSqlSource(config, writerContext)

    val sqlSourceData = DataframeBuilder().buildDataFrame(jdbcDF, config.getDataframeBuilderContext)

    val writerService = getWriteService(config, writerContext)
    writerService.write(sqlSourceData)

    deltaPostProcessing(config, sqlSourceData, writerService.getUserMetadataJSON)
  }

  private def setSparkContext(config: Config): Unit = {

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

    lazy val changeTrackingHandler: ChangeTrackingHandler = new ChangeTrackingHandler(
      config,
      getJdbcChangeTrackingService(config)
    ) with SparkContextTrait

    val isDeltaTableExists: Boolean =
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
        val jdbcCTService = new JdbcCTService(jdbcContext) with JdbcBuilder with SparkContextTrait
        jdbcCTService.loadData()
      } else {
        val jdbcService: JdbcService = getJdbcService(config, jdbcContext)
        jdbcService.loadData(getQuery)
      }
    jdbcDF
  }

  private def getJdbcChangeTrackingService(config: Config) = {
    new JdbcCTService(config.getJdbcContext) with JdbcBuilder with SparkContextTrait
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
