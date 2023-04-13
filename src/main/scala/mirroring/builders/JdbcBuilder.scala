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

package mirroring.builders

import java.sql.{CallableStatement, Connection, ResultSet}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import mirroring.builders.SqlBuilder.buildSQLObjectName
import mirroring.services.SparkService.spark
import mirroring.services.databases.JdbcContext

import scala.collection.mutable.ListBuffer
import wvlet.log.LogSupport

object JdbcBuilder extends LogSupport {

  def buildJDBCResultSet(
      connection: Connection,
      query: String,
      parameters: Array[String] = Array[String]()
  ): ResultSet = {
    val cStmt: CallableStatement = connection.prepareCall(query)
    for ((parameter, i) <- parameters.zipWithIndex) {
      cStmt.setString(i + 1, parameter)
    }
    cStmt.execute()
    val rs: ResultSet = cStmt.getResultSet
    rs
  }

  def buildStructFromResultSet(rs: ResultSet): StructType = {
    val md                                        = rs.getMetaData
    val columnCount                               = md.getColumnCount
    val structFieldsList: ListBuffer[StructField] = new ListBuffer[StructField]()
    for (i <- 1 to columnCount) {
      structFieldsList += StructField(
        md.getColumnName(i),
        StringType,
        if (md.isNullable(i) == 1) true else false
      )
    }
    val schema = StructType(structFieldsList.toList)
    schema
  }

  def buildDataFrameFromResultSet(rs: ResultSet): DataFrame = {
    // Prepare a schema and columns
    val schema = buildStructFromResultSet(rs)
    val columns = schema.foldLeft(Seq.empty[String]) { (seq: Seq[String], col: StructField) =>
      seq ++ Seq(col.name)
    }
    // generate DataFrame
    val df: DataFrame = parallelizeResultSet(rs, columns, schema, spark)
    df
  }

  // Define how each record will be converted in the ResultSet to a Row at each iteration
  private def parseResultSet(rs: ResultSet, columns: Seq[String]): Row = {
    val resultSetRecord = columns.map(c => rs.getString(c))
    Row(resultSetRecord: _*)
  }

  private def resultSetToIter(rs: ResultSet, columns: Seq[String])(
      f: (ResultSet, Seq[String]) => Row
  ): Iterator[Row] =
    new Iterator[Row] {
      def hasNext: Boolean = rs.next()
      def next(): Row      = f(rs, columns)
    }

  private def parallelizeResultSet(
      rs: ResultSet,
      columns: Seq[String],
      schema: StructType,
      sparkSession: SparkSession
  ): DataFrame = {
    val rdd =
      sparkSession.sparkContext.parallelize(resultSetToIter(rs, columns)(parseResultSet).toSeq)
    sparkSession.createDataFrame(rdd, schema)
  }

  def buildCTQueryParams(
      CTChangesQueryParams: Array[String],
      jdbcContext: JdbcContext
  ): Array[String] = {
    var params: Array[String] = Array()
    for (param <- CTChangesQueryParams) {
      param match {
        case "defaultSQLTable" =>
          logger.info("Change Tracking default param used: SQLTable")
          params :+= buildSQLObjectName(jdbcContext.schema, jdbcContext.table)
        case "queryCTLastVersion" =>
          logger.info("Change Tracking default param used: querying last version")
          params :+= jdbcContext.ctLastVersion.toString
        case "queryCTCurrentVersion" =>
          logger.info("Change Tracking default param used: querying current version")
          params :+= jdbcContext.ctCurrentVersion.toString
        case _ =>
          params :+= param
      }
    }
    params
  }

}
