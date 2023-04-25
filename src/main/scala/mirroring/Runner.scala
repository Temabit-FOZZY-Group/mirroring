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
import org.apache.spark.sql.DataFrame
import mirroring.builders.{ConfigBuilder, DataframeBuilder, FilterBuilder}
import mirroring.handlers.ChangeTrackingHandler
import mirroring.services.databases.{JdbcCTService, JdbcPartitionedService, JdbcService}
import mirroring.services.writer.{ChangeTrackingService, DeltaService, MergeService, WriterContext}
import mirroring.services.{DeltaTableService, SparkService, SqlService}
import wvlet.log.LogSupport

object Runner extends LogSupport {

  def initConfig(args: Array[String]): Config = {
    val config: Config = ConfigBuilder.build(ConfigBuilder.parse(args))
    logger.debug(s"Parameters parsed: ${config.toString}")
    config
  }

  def setSparkContext(config: Config): Unit = {
    val spark = SparkService.spark
    logger.info(
      s"""Creating spark session with configurations: ${spark.conf.getAll
        .mkString(", ")}"""
    )
    spark.sparkContext.setLogLevel(config.logSparkLvl)
    spark.conf.set("spark.sql.session.timeZone", config.timezone)
  }

  def main(args: Array[String]): Unit = {
    // preliminary FlowLogger initialization in order to log config building
    FlowLogger.init("schema", "tab", "info")
    val config: Config = initConfig(args)
    logger.info("Starting mirroring-lib...")
    setSparkContext(config)
    val jdbcContext                                  = config.getJdbcContext
    val writerContext: WriterContext                 = config.getWriterContext
    var query: String                                = config.query
    val changeTrackingHandler: ChangeTrackingHandler = new ChangeTrackingHandler(config)
    lazy val isDeltaTableExists: Boolean = DeltaTable.isDeltaTable(
      SparkService.spark,
      config.pathToSave
    )
    var userMetadataJSON: String = ""

    val jdbcDF: DataFrame =
      if (config.isChangeTrackingEnabled && isDeltaTableExists && config.CTChangesQuery.nonEmpty) {
        changeTrackingHandler.changeTrackingFlow(isDeltaTableExists, writerContext, jdbcContext)
        logger.info("Change Tracking: use custom ctChangesQuery")
        JdbcCTService.loadData(jdbcContext)
      } else {
        if (config.isChangeTrackingEnabled) {
          changeTrackingHandler.changeTrackingFlow(isDeltaTableExists, writerContext, jdbcContext)
          query = changeTrackingHandler.query(isDeltaTableExists)
        }
        var jdbcService: JdbcService = new JdbcService(jdbcContext)
        if (config.splitBy.nonEmpty) {
          jdbcService = new JdbcPartitionedService(jdbcContext)
        }
        jdbcService.loadData(query)
      }

    logger.info(s"Number of incoming rows: ${jdbcDF.count}")

    val ds = DataframeBuilder.buildDataFrame(jdbcDF, config.getDataframeBuilderContext).cache()
    jdbcDF.unpersist()

    var writerService: DeltaService = new DeltaService(writerContext)
    if (config.isChangeTrackingEnabled) {
      writerService = new ChangeTrackingService(writerContext)
      userMetadataJSON = writerService.getUserMetadataJSON
    } else if (config.useMerge) {
      writerService = new MergeService(writerContext)
    }
    writerService.write(data = ds)
    deltaPostProcessing(config, ds, userMetadataJSON)
  }

  def deltaPostProcessing(config: Config, ds: DataFrame, userMetadataJSON: String): Unit = {
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
