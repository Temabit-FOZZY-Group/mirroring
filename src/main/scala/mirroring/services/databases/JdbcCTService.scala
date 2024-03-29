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
import mirroring.builders._
import mirroring.services.SparkContextTrait
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.implicits.longEncoder
import org.apache.spark.sql.types.StructType
import wvlet.log.Logger

import java.sql.{Connection, DriverManager, ResultSet}

class JdbcCTService(jdbcContext: JdbcContext) extends Serializable {
  this: JdbcBuilder with SparkContextTrait =>

  val log: Logger = Logger.of[JdbcCTService]

  def loadData(): DataFrame = {

    class ConnectionManager extends JdbcRDD.ConnectionFactory {
      override def getConnection: Connection = {
        DriverManager.getConnection(jdbcContext.url)
      }
    }
    val connectionManager: ConnectionManager = new ConnectionManager
    val params: Array[String] = buildCTQueryParams(
      jdbcContext.ctChangesQueryParams,
      jdbcContext
    )
    val resultSet: ResultSet = getSchema(connectionManager)
    try {
      val schema: StructType = buildStructFromResultSet(resultSet)
      log.debug(schema)
      log.info("Executing procedure to create rdd...")
      // TO DO: refactor into val moving repartition here
      var myRDD: JavaRDD[Array[Object]] = JdbcRDD
        .create(
          getSparkSession.sparkContext,
          connectionManager,
          jdbcContext.ctChangesQuery,
          params(0).toLong,
          params(1).toLong,
          1,
          r => JdbcRDD.resultSetToObjectArray(r)
        )
      log.info("Repartitioning...")
      myRDD = myRDD.repartition(getSparkSession.conf.get("spark.sql.shuffle.partitions").toInt)
      log.info(s"getNumPartitions: ${myRDD.getNumPartitions}. Building DataFrame from rdd...")
      buildDataFrameFromRDD(
        myRDD,
        schema
      )
    } catch {
      case e: Exception =>
        log.error(
          s"Error executing ${jdbcContext.ctChangesQuery} with params: ${params.mkString(", ")}"
        )
        throw e
    }
  }

  private def getSchema(cm: JdbcRDD.ConnectionFactory): ResultSet = {
    val maxValueParams: Array[String] = Array(Long.MaxValue.toString, Long.MaxValue.toString)
    val resultSet: ResultSet =
      try {
        log.info("Executing procedure with Long.MaxValue to get schema...")
        getResultSet(
          cm.getConnection,
          jdbcContext.ctChangesQuery,
          maxValueParams
        )
      } catch {
        case e: Exception =>
          log.error(
            s"Error executing ${jdbcContext.ctChangesQuery} with params: ${maxValueParams.mkString(", ")}"
          )
          throw e
      }
    resultSet
  }

  /** Returns value from the first row, first column of the result set as BigInt.
    *
    * Use to get Change Tracking version from the result set.
    */
  def getChangeTrackingVersion(query: String, parameters: Array[String]): BigInt = {
    val connection = DriverManager.getConnection(jdbcContext.url)
    try {
      val params: Array[String] = buildCTQueryParams(
        parameters,
        jdbcContext
      )
      val rs = getResultSet(
        connection,
        query,
        params
      )
      rs.next()
      rs.getLong(1)
    } catch {
      case e: Exception =>
        log.error(s"Error getting Change Tracking version using query $query")
        throw e
    } finally {
      connection.close()
    }
  }

  def getChangeTrackingVersion(query: String): BigInt = {
    var version: BigInt = BigInt(0)
    try {
      version = getSparkSession.read
        .format("jdbc")
        .option("url", jdbcContext.url)
        .option("dbtable", query)
        .load()
        .as[Long]
        .first()
    } catch {
      case e: java.lang.NullPointerException =>
        log.error(s"Change tracking is not enabled on queried table: $query")
        throw e
    }
    version
  }
}
