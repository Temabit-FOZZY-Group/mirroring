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

import org.apache.spark.sql.DataFrame
import palefat.data.mirror.builders._
import wvlet.log.LogSupport

import java.sql.DriverManager

class JdbcCTService(jdbcContext: JdbcContext) extends JdbcService(jdbcContext) with LogSupport {

  override def loadData(@annotation.unused _query: String = ""): DataFrame = {
    val connection = DriverManager.getConnection(url)
    try {
      val params: Array[String] = JdbcBuilder.buildCTChangesQueryParams(
        jdbcContext.ctChangesQueryParams,
        jdbcContext.schema,
        jdbcContext.table,
        jdbcContext.ctLastVersion.toString,
        jdbcContext.ctCurrentVersion.toString,
      )
      val jdbcDF: DataFrame = JdbcBuilder.buildDataFrameFromResultSet(
        JdbcBuilder.buildJDBCResultSet(
          connection,
          jdbcContext.ctChangesQuery,
          params
        )
      )
      // spark.createDataFrame is lazy so action on jdbcDF is needed while ResultSet is open
      logger.info(s"Number of incoming rows: ${jdbcDF.count}")
      jdbcDF
    } catch {
      case e: Exception => throw e
    } finally {
      connection.close()
    }
  }

  /**
   * Returns value from the first row, first column of the result set as BigInt.
   *
   * Use to get Change Tracking version from the result set.
   */
  def getChangeTrackingVersion(query: String,
                               parameters: Array[String] = Array[String](),
                                    ): BigInt = {
    val connection = DriverManager.getConnection(url)
    try {
      val rs = JdbcBuilder.buildJDBCResultSet(
        connection,
        query,
        parameters
      )
      rs.next()
      rs.getLong(1)
    } catch {
      case e: Exception => throw e
    } finally {
      connection.close()
    }
  }

  def getChangeTrackingVersion(query: String): BigInt = {
    val jdbcDF: DataFrame = super[JdbcService].loadData(query).cache()

    var version: BigInt = BigInt(0)

    if (!jdbcDF.isEmpty) {
      version = BigInt(
        jdbcDF
          .collect()(0)
          .getLong(0)
      )
    }
    version
  }
}
