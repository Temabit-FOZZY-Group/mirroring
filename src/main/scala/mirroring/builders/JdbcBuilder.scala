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
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  TimestampType,
  DoubleType,
  StructField,
  StructType
}
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SQLContext, SparkSession}
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

  def buildDataFrameFromResultSetTest(rs: ResultSet): DataFrame = {
    val rowList     = new scala.collection.mutable.MutableList[Row]
    var cRow: Row   = null
    val md          = rs.getMetaData
    val columnCount = md.getColumnCount
    // Prepare a schema and columns
    val schema = getSchema(rs)
    //Looping resultset
    while (rs.next()) {
      //adding columns into a "Row" object
      cRow = RowFactory.create(
        rs.getObject(1),
        rs.getObject(2),
        rs.getObject(3),
        rs.getObject(4),
        rs.getObject(5),
        rs.getObject(6),
        rs.getObject(7),
        rs.getObject(8)
      )
      //adding each rows into "List" object.
      rowList += (cRow)
    }
    // generate DataFrame
    val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(rowList), schema)
    df
  }

  def getSchema(rs: ResultSet): StructType = {
    val schema = StructType(
      StructField("activityid", IntegerType, true) ::
        StructField("FilId", IntegerType, true) ::
        StructField("activityName", StringType, true) ::
        StructField("created", TimestampType, true) ::
        StructField("val", DoubleType, true) ::
        StructField("SYS_CHANGE_OPERATION", StringType, true) ::
        StructField("SYS_CHANGE_PK_activityId", IntegerType, true) ::
        StructField("SYS_CHANGE_PK_FilId", IntegerType, true) :: Nil
    )
    schema
  }

}
