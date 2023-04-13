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
import mirroring.Runner.getUrl
import mirroring.builders._
import mirroring.services.SparkService.spark
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import wvlet.log.LogSupport

import java.sql.{Connection, DriverManager, ResultSet}

object JdbcCTService extends LogSupport {

  def loadData(jdbcContext: JdbcContext, url: String): DataFrame = {
    class ConnectionMngr extends JdbcRDD.ConnectionFactory {
      override def getConnection: Connection = {
        DriverManager.getConnection(url)
      }
    }
    val cm: ConnectionMngr = new ConnectionMngr
    val params: Array[String] = JdbcBuilder.buildCTQueryParams(
      jdbcContext.ctChangesQueryParams,
      jdbcContext
    )
    try {
      logger.info("Extracting result set...")
      val resultSet: ResultSet = JdbcBuilder.buildJDBCResultSet(
        cm.getConnection,
        jdbcContext.ctChangesQuery,
        Array(Long.MaxValue.toString, Long.MaxValue.toString)
      )
      val schema: StructType = JdbcBuilder.buildStructFromResultSet(resultSet)
      val myRDD: JavaRDD[Array[Object]] = JdbcRDD.create(
        spark.sparkContext,
        cm,
        jdbcContext.ctChangesQuery,
        params(0).toLong,
        params(1).toLong,
        1,
        r => JdbcRDD.resultSetToObjectArray(r)
      )
      logger.info("Building DataFrame from result set...")
      val jdbcDF: DataFrame = JdbcBuilder.buildDataFrameFromRDD(myRDD, schema)
      // spark.createDataFrame is lazy so action on jdbcDF is needed while ResultSet is open
      logger.info(s"Number of incoming rows: ${jdbcDF.count}")
      jdbcDF
    } catch {
      case e: Exception =>
        logger.error(
          s"Error executing ${jdbcContext.ctChangesQuery} with params: ${params.mkString}"
        )
        throw e
    }
  }

  /** Returns value from the first row, first column of the result set as BigInt.
    *
    * Use to get Change Tracking version from the result set.
    */
  def getChangeTrackingVersion(
      query: String,
      parameters: Array[String],
      jdbcContext: JdbcContext
  ): BigInt = {
    val url: String = getUrl(jdbcContext)
    val connection  = DriverManager.getConnection(url)
    try {
      val params: Array[String] = JdbcBuilder.buildCTQueryParams(
        parameters,
        jdbcContext
      )
      val rs = JdbcBuilder.buildJDBCResultSet(
        connection,
        query,
        params
      )
      rs.next()
      rs.getLong(1)
    } catch {
      case e: Exception =>
        logger.error(s"Error getting Change Tracking version using query $query")
        throw e
    } finally {
      connection.close()
    }
  }

  def getChangeTrackingVersion(query: String, jdbcContext: JdbcContext): BigInt = {
    val jdbcService: JdbcService = new JdbcService(jdbcContext)
    val jdbcDF: DataFrame        = jdbcService.loadData(query).cache()

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
