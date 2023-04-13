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

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.DataFrame
import mirroring.builders._
import mirroring.services.SparkService.spark
import wvlet.log.LogSupport

import java.sql.{DriverManager, ResultSet}

class JdbcCTService(jdbcContext: JdbcContext) extends JdbcService(jdbcContext) with LogSupport {

  override def loadData(@annotation.unused _query: String = ""): DataFrame = {
    @transient lazy val connection1 = DriverManager.getConnection(url)
    @transient lazy val connection  = DriverManager.getConnection(url)
    val params: Array[String] = JdbcBuilder.buildCTQueryParams(
      jdbcContext.ctChangesQueryParams,
      jdbcContext
    )
    try {
      logger.info(JdbcBuilder.getSchema())
      logger.info("Extracting result set...")
      val resultSet: ResultSet = JdbcBuilder.buildJDBCResultSet(
        connection1,
        jdbcContext.ctChangesQuery,
        Array(Long.MaxValue.toString, Long.MaxValue.toString)
      )
      val schema = JdbcBuilder.buildStructFromResultSet(resultSet)
      val myRDD = new JdbcRDD(
        spark.sparkContext,
        () => connection,
        jdbcContext.ctChangesQuery,
        params(0).toInt,
        params(1).toInt,
        1,
        r => JdbcBuilder.resultSetToStringArray(r)
      )
      logger.info("Building DataFrame from result set...")
      val jdbcDF: DataFrame = JdbcBuilder.buildDataFrameFromRDD(myRDD, schema).cache()
      // spark.createDataFrame is lazy so action on jdbcDF is needed while ResultSet is open
      logger.info(s"Number of incoming rows: ${jdbcDF.count}")
      jdbcDF
    } catch {
      case e: Exception =>
        logger.error(
          s"Error executing ${jdbcContext.ctChangesQuery} with params: ${params.mkString}"
        )
        throw e
    } finally {
      connection.close()
    }
  }

  /** Returns value from the first row, first column of the result set as BigInt.
    *
    * Use to get Change Tracking version from the result set.
    */
  def getChangeTrackingVersion(
      query: String,
      parameters: Array[String] = Array[String]()
  ): BigInt = {
    val connection = DriverManager.getConnection(url)
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
