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

import org.apache.spark.sql.{DataFrame, DataFrameReader, Encoders}
import mirroring.DatatypeMapping
import mirroring.services.SparkService.spark
import wvlet.log.LogSupport

class JdbcService(jdbcContext: JdbcContext) extends LogSupport {

  protected lazy val customSchema: String = {
    val sql =
      s"(select column_name, data_type from INFORMATION_SCHEMA.COLUMNS with (nolock) where " +
        s"TABLE_NAME = '${jdbcContext.table}' and TABLE_SCHEMA = '${jdbcContext.schema}') as subq"

    val sourceSchema =
      spark.read.format("jdbc").option("url", jdbcContext.url).option("dbtable", sql).load()

    // create custom schema to avoid transferring DATE as STRING
    // viz https://jtds.sourceforge.net/typemap.html
    try {
      val customSchema = sourceSchema
        .map(row => {
          if (DatatypeMapping.contains(row.getString(1))) {
            f"${row.getString(0)} ${DatatypeMapping.getDatatype(row.getString(1))}"
          } else {
            ""
          }
        })(Encoders.STRING)
        .filter(row => row.nonEmpty)
        .reduce(_ + ", " + _)

      logger.info(s"Reading data with customSchema: $customSchema")
      customSchema
    } catch {
      case e: java.lang.UnsupportedOperationException
          if e.getMessage.contains("empty collection") =>
        logger.info(s"No custom schema will be used")
        ""
    }
  }
  def loadData(_query: String): DataFrame = {
    logger.info(s"Reading data with query: ${_query.linesIterator.mkString(" ").trim}")
    dfReader.option("dbtable", _query).load().cache()
  }

  def dfReader: DataFrameReader = {
    spark.read
      .format("jdbc")
      .option("url", jdbcContext.url)
      .option("customSchema", customSchema)
  }
}
