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
import palefat.data.mirror.services.databases.{DbService, JdbcPartitionedDecorator, JdbcService}
import palefat.data.mirror.services.writer._
import palefat.data.mirror.services.{DeltaTableService, SqlService}
import wvlet.log.LogSupport

object Runner extends LogSupport {

  def main(args: Array[String]): Unit = {

    val config: Config = ConfigBuilder.build(ConfigBuilder.parse(args))
    logger.debug(s"Parameters parsed: ${config.toString}")
    logger.info(
      s"""Creating spark session with configurations: ${spark.conf.getAll
        .mkString(", ")}"""
    )

    spark.sparkContext.setLogLevel(config.logSparkLvl)
    spark.conf.set("spark.sql.session.timeZone", config.timezone)

    logger.info("Starting mirroring-lib...")

    // Building contexts from the configuration
    val dataframeBuilderContext      = config.getDataframeBuilderContext
    val jdbcContext                  = config.getJdbcContext
    val writerContext: WriterContext = config.getWriterContext

    // Getting query to retrieve data
    var query: String         = config.query
    val changeTrackingHandler = new ChangeTrackingHandler(config)
    if (config.isChangeTrackingEnabled) {
      query = changeTrackingHandler.query
      writerContext.ctCurrentVersion = changeTrackingHandler.ctCurrentVersion
    }

    // Creating service that will load data
    var jdbcService: DbService = new JdbcService(jdbcContext)

    if (config.splitBy.nonEmpty) {
      jdbcService = new JdbcPartitionedDecorator(jdbcService, jdbcContext)
    }

    // Loading data
    val jdbcDF: DataFrame = jdbcService.loadData(query).cache()
    logger.info(s"Number of incoming rows: ${jdbcDF.count}")

    // Building DataFrame, i.e. casting types, adding/renaming columns
    val ds =
      DataframeBuilder.buildDataFrame(jdbcDF, dataframeBuilderContext).cache()
    jdbcDF.unpersist()

    // Creating service that will write data to the storage
    var writerService: DeltaService = new DeltaService(writerContext)
    if (config.isChangeTrackingEnabled) {
      writerService = new ChangeTrackingService(writerContext)
    } else if (config.useMerge) {
      writerService = new MergeService(writerContext)
    }

    // Writing data
    writerService.write(data = ds)

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
