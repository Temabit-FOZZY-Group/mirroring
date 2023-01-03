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

package palefat.data.mirror

import org.apache.spark.sql.DataFrame
import palefat.data.mirror.builders.{ConfigBuilder, DataframeBuilder, FilterBuilder}
import palefat.data.mirror.handlers.ChangeTrackingHandler
import palefat.data.mirror.services.SparkService.spark
import palefat.data.mirror.services.databases.{JdbcCTService, JdbcPartitionedService, JdbcService}
import palefat.data.mirror.services.writer.{ChangeTrackingService, DeltaService, MergeService, WriterContext}
import palefat.data.mirror.services.{DeltaTableService, SqlService}
import wvlet.log.LogSupport

object Runner extends LogSupport {

  def initConfig(args: Array[String]): Config = {
    val config: Config = ConfigBuilder.build(ConfigBuilder.parse(args))
    logger.debug(s"Parameters parsed: ${config.toString}")
    config
  }

  def setSparkContext(config: Config): Unit = {
    logger.info(
      s"""Creating spark session with configurations: ${spark.conf.getAll
        .mkString(", ")}"""
    )
    spark.sparkContext.setLogLevel(config.logSparkLvl)
    spark.conf.set("spark.sql.session.timeZone", config.timezone)
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting mirroring-lib...")
    val config: Config = initConfig(args)
    setSparkContext(config)
    val jdbcContext                  = config.getJdbcContext
    val writerContext: WriterContext = config.getWriterContext
    var query: String                = config.query
    val changeTrackingHandler        = new ChangeTrackingHandler(config)

    if (config.isChangeTrackingEnabled) {
      query = changeTrackingHandler.query
      writerContext.ctCurrentVersion = changeTrackingHandler.ctCurrentVersion
      jdbcContext._ctCurrentVersion = Some(changeTrackingHandler.ctCurrentVersion)
      jdbcContext._changeTrackingLastVersion = Some(changeTrackingHandler.changeTrackingLastVersion)
    }

    val jdbcDF: DataFrame = if (config.CTChangesQuery.isEmpty) {
      var jdbcService: JdbcService = new JdbcService(jdbcContext)
      if (config.splitBy.nonEmpty) {
        jdbcService = new JdbcPartitionedService(jdbcContext)
      }
      val jdbcDFTemp = jdbcService.loadData(query).cache()
      logger.info(s"Number of incoming rows: ${jdbcDFTemp.count}")
      jdbcDFTemp
    } else {
      logger.info("Change Tracking: use custom ctChangesQuery")
      val jdbcCTService: JdbcCTService = new JdbcCTService(jdbcContext)
      jdbcCTService.loadData()
    }
    val ds = DataframeBuilder.buildDataFrame(jdbcDF, config.getDataframeBuilderContext).cache()
    jdbcDF.unpersist()

    var writerService: DeltaService = new DeltaService(writerContext)
    if (config.isChangeTrackingEnabled) {
      writerService = new ChangeTrackingService(writerContext)
    } else if (config.useMerge) {
      writerService = new MergeService(writerContext)
    }
    writerService.write(data = ds)
    deltaPostProcessing(config, ds)
  }

  def deltaPostProcessing(config: Config, ds: DataFrame): Unit = {
    if (config.zorderby_col.nonEmpty) {
      val replaceWhere =
        FilterBuilder.buildReplaceWherePredicate(
          ds,
          config.lastPartitionCol,
          config.whereClause.toString
        )
      DeltaTableService.executeZOrdering(
        config.pathToSave,
        config.zorderby_col,
        replaceWhere
      )
    }
    DeltaTableService.runVacuum(config.pathToSave)
    if (config.hiveDb.nonEmpty) {
      SqlService.run(config)
    }
  }
}
