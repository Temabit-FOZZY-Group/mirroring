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
import mirroring.services.databases.{
  JdbcCTService,
  JdbcContext,
  JdbcPartitionedService,
  JdbcService
}
import mirroring.services.writer.{ChangeTrackingService, DeltaService, MergeService, WriterContext}
import mirroring.services.{DeltaTableService, SparkService, SqlService}

import scala.collection.mutable
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

    val jdbcDF: DataFrame =
      if (config.isChangeTrackingEnabled && isDeltaTableExists && config.CTChangesQuery.nonEmpty) {
        changeTrackingHandler.changeTrackingFlow(isDeltaTableExists, writerContext, jdbcContext)
        logger.info("Change Tracking: use custom ctChangesQuery")
        val url: String = getUrl(jdbcContext)
        JdbcCTService.loadData(jdbcContext, url)
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
      SqlService.run(config)
    }
  }

  def getUrl(jdbcContext: JdbcContext): String = {
    val MssqlUser: String     = sys.env.getOrElse("MSSQL_USER", "")
    val MssqlPassword: String = sys.env.getOrElse("MSSQL_PASSWORD", "")

    lazy val url: String = {
      // If user/password are passed through environment variables, extract them and append to the url
      val sb = new mutable.StringBuilder(jdbcContext.url)
      if (
        !jdbcContext.url.contains("user") && !jdbcContext.url.contains(
          "password"
        ) && MssqlUser.nonEmpty && MssqlPassword.nonEmpty
      ) {
        if (!jdbcContext.url.endsWith(";")) {
          sb.append(";")
        }
        sb.append(s"user=$MssqlUser;password=$MssqlPassword")
      }
      require(
        sb.toString.contains("password="),
        "Parameters user and password are required for jdbc connection."
      )
      sb.toString
    }
    url
  }
}
