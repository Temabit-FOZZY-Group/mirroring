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
import org.apache.spark.sql.delta.implicits.stringEncoder
import org.apache.spark.sql.{DataFrame, DataFrameReader}
import wvlet.log.LogSupport

class JdbcService(jdbcContext: JdbcContext) extends LogSupport {
  this: SparkContextTrait =>

  protected lazy val customSchema: String = getCustomSchema

  def loadData(query: String): DataFrame = {
    logger.info(s"Reading data with query: ${query.linesIterator.mkString(" ").trim}")

    getDataFrameReader
      .option("dbtable", query)
      .load()
  }

  def getDataFrameReader: DataFrameReader = {
    getSparkSession.read
      .format("jdbc")
      .option("url", jdbcContext.url)
      .option("customSchema", customSchema)
  }

  private def getCustomSchema: String = {
    // create custom schema to avoid transferring DATE as STRING
    // viz https://jtds.sourceforge.net/typemap.html
    val getDateColumnsQuery =
      s"""(select concat(column_name, ' ', data_type) as res from INFORMATION_SCHEMA.COLUMNS with (nolock) where
       |TABLE_NAME = '${jdbcContext.table}' and TABLE_SCHEMA = '${jdbcContext.schema}'
       |and DATA_TYPE IN ('date', 'datetime2')) as subq
       |""".stripMargin

    val sourceSchema = executeQuery(getDateColumnsQuery).as[String]

    try {
      val customSchema = sourceSchema.reduce(_ + ", " + _)
      logger.info(s"Reading data with customSchema: $customSchema")
      customSchema.replaceAll("datetime2", "timestamp")
    } catch {
      case e: java.lang.UnsupportedOperationException
          if e.getMessage.contains("empty collection") =>
        logger.info(s"No custom schema will be used")
        ""
    }
  }

  private def executeQuery(sql: String): DataFrame = {
    getSparkSession.read
      .format("jdbc")
      .option("url", jdbcContext.url)
      .option("dbtable", sql)
      .load()
  }
}
