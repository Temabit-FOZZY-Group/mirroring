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

import org.apache.spark.sql.functions.{col, date_format, date_trunc}
import org.apache.spark.sql.{DataFrame, Encoders}
import wvlet.log.LogSupport

import scala.collection.mutable

class JdbcPartitionedService(
    context: JdbcContext
) extends JdbcService(context)
    with LogSupport {

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

  private var _query = ""

  def query: String = _query

  def query_=(in: String): Unit = _query = in

  private lazy val lowerBound: String = {
    val sql =
      s"""(SELECT MIN(subq.${context.partitionColumn}) AS lowerBound FROM
         |$query) as query
      """.stripMargin
    var ds = super[JdbcService].loadData(sql)
    // Format timestamp to avoid Conversion failed when converting date and/or time from character string.
    if (ds.schema("lowerBound").dataType.simpleString == "timestamp") {
      ds = ds.withColumn(
        "lowerBound",
        date_format(
          date_trunc("day", col("lowerBound")),
          "yyyy-MM-dd HH:mm:ss.SSS"
        )
      )
    }

    try {
      ds.as[String](Encoders.STRING).first
    } catch {
      case e: java.lang.NullPointerException =>
        ""
    }
  }

  private lazy val upperBound: String = {
    val sql =
      s"""(SELECT MAX(subq.${context.partitionColumn}) AS upperBound FROM
         |$query) as query
      """.stripMargin

    var ds = super[JdbcService].loadData(sql)

    // Format timestamp to avoid Conversion failed when converting date and/or time from character string.
    if (ds.schema("upperBound").dataType.simpleString == "timestamp") {
      ds = ds.withColumn(
        "upperBound",
        date_format(
          date_trunc("day", col("upperBound")),
          "yyyy-MM-dd HH:mm:ss.SSS"
        )
      )
    }

    try {
      ds.as[String](Encoders.STRING).first
    } catch {
      case e: java.lang.NullPointerException =>
        ""
    }
  }

  override def loadData(_query: String): DataFrame = {
    logger.info(s"Reading data with query: ${_query}")
    // setting query to use it in the lower/upper bounds calculations
    query = _query
    dfReader.options(options).option("dbtable", _query).load()
  }
}
