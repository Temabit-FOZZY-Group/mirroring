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

package mirroring.services.databases

import mirroring.services.SparkContextTrait
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, date_trunc}
import wvlet.log.Logger

import scala.collection.mutable

class JdbcPartitionedService(context: JdbcContext) extends JdbcService(context) {
  this: SparkContextTrait =>

  override val logger: Logger = Logger.of[JdbcPartitionedService]

  private lazy val options: mutable.Map[String, String] = {
    var options = mutable.Map[String, String]()
    // if bounds are empty (due to no data on the source), do not pass options
    if (lowerBound != null && lowerBound.nonEmpty) {
      options = mutable.Map[String, String](
        "partitionColumn" -> context.partitionColumn,
        "numPartitions"   -> context.numPartitions,
        "lowerBound"      -> lowerBound,
        "upperBound"      -> upperBound
      )
    }
    logger.info(s"Reading data with options: ${options.mkString(", ")}")
    options
  }

  private lazy val lowerBound: String = getBounds().first().getString(1)

  private lazy val upperBound: String = getBounds().first().getString(0)

  private var _query = ""

  def query: String = _query

  def query_=(in: String): Unit = _query = in

  private def getBounds(): DataFrame = {
    val sql =
      s"""(SELECT
         |MAX(subq.${context.partitionColumn}) AS MAX,
         |MIN(subq.${context.partitionColumn}) AS MIN
         |FROM $query) as query
      """.stripMargin

    var ds: DataFrame = this.getSparkSession.read
      .format("jdbc")
      .option("url", context.url)
      .option("dbtable", sql)
      .load()

    // Format timestamp to avoid Conversion failed when converting date and/or time from character string.
    ds.columns
      .foreach { columnName =>
        ds = addDayPrecisionTimestamp(ds, columnName)
          .withColumn(columnName, col(columnName).cast("string"))
      }
    ds
  }

  override def loadData(_query: String): DataFrame = {
    logger.info(s"Reading data with query: ${_query}")
    // setting query to use it in the lower/upper bounds calculations
    query = _query
    getDataFrameReader
      .options(options)
      .option("dbtable", _query)
      .load()
  }

  private def addDayPrecisionTimestamp(ds: DataFrame, columnName: String): DataFrame = {
    if (columnHasTimestampType(ds, columnName)) {
      ds.withColumn(
        columnName,
        date_format(
          date_trunc("day", col(columnName)),
          "yyyy-MM-dd HH:mm:ss.SSS"
        )
      )
    } else {
      ds
    }
  }

  private def columnHasTimestampType(ds: DataFrame, columnName: String): Boolean = {
    ds.schema(columnName).dataType.simpleString == "timestamp"
  }
}
