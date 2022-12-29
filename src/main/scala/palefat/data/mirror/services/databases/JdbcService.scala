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

package palefat.data.mirror.services.databases

import org.apache.spark.sql.{DataFrame, DataFrameReader, Encoders}
import palefat.data.mirror.DatatypeMapping
import palefat.data.mirror.services.SparkService.spark
import wvlet.log.LogSupport

import scala.collection.mutable

class JdbcService(jdbcContext: JdbcContext) extends DbService with LogSupport {

  val MssqlUser: String     = sys.env.getOrElse("MSSQL_USER", "")
  val MssqlPassword: String = sys.env.getOrElse("MSSQL_PASSWORD", "")

  override lazy val url: String = {
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

  private lazy val customSchema: String = {
    val sql =
      s"(select column_name, data_type from INFORMATION_SCHEMA.COLUMNS with (nolock) where " +
        s"TABLE_NAME = '${jdbcContext.table}' and TABLE_SCHEMA = '${jdbcContext.schema}') as subq"

    val sourceSchema =
      spark.read.format("jdbc").option("url", url).option("dbtable", sql).load()

    // create custom schema to avoid transferring DATE as STRING
    // viz https://jtds.sourceforge.net/typemap.html
    try {
      val customSchema = sourceSchema
        .map(row => {
          if (DatatypeMapping.contains(row.getString(1))) {
            f"${row.getString(0)} ${DatatypeMapping.getDatatype(row.getString(1))}"
          } else { "" }
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

  override def loadData(_query: String): DataFrame = {
    logger.info(s"Reading data with query: ${_query}")
    dfReader.option("dbtable", _query).load()
  }

  override def dfReader: DataFrameReader = {
    spark.read
      .format("jdbc")
      .option("url", url)
      .option("customSchema", customSchema)
  }
}
