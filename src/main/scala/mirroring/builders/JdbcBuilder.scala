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

import mirroring.builders.SqlBuilder.buildSQLObjectName
import mirroring.services.SparkService.spark
import mirroring.services.databases.JdbcContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import wvlet.log.LogSupport

import java.sql.{CallableStatement, Connection, ResultSet, ResultSetMetaData}

object JdbcBuilder extends LogSupport {

  def getResultSet(connection: Connection, query: String, parameters: Array[String] = Array()): ResultSet = {

    val statement: CallableStatement = connection.prepareCall(query)

    parameters.zipWithIndex.foreach {
      case (parameter, i) => statement.setString(i + 1, parameter)
    }
    statement.execute()

    val rs: ResultSet = statement.getResultSet
    rs
  }

  def buildStructFromResultSet(rs: ResultSet): StructType = {

    val md = rs.getMetaData
    val columnCount = md.getColumnCount

    val structFieldsList: List[StructField] =
      (1 to columnCount)
      .map(columnNumber => getStructField(md, columnNumber))
      .toList

    StructType(structFieldsList)
  }

  private def getStructField(md: ResultSetMetaData, columnNumber: Int) = {
    StructField(
      md.getColumnName(columnNumber),
      fromJavaSQLType(md.getColumnType(columnNumber), md.getPrecision(columnNumber), md.getScale(columnNumber)),
      md.isNullable(columnNumber) == 1
    )
  }

  def buildCTQueryParams(CTChangesQueryParams: Array[String], jdbcContext: JdbcContext): Array[String] = {

    CTChangesQueryParams.map {
      case "defaultSQLTable" =>
        logger.info("Change Tracking default param used: SQLTable")
        buildSQLObjectName(jdbcContext.schema, jdbcContext.table)
      case "queryCTLastVersion" =>
        logger.info("Change Tracking default param used: querying last version")
        jdbcContext.ctLastVersion.toString
      case "queryCTCurrentVersion" =>
        logger.info("Change Tracking default param used: querying current version")
        jdbcContext.ctCurrentVersion.toString
      case otherParam => otherParam
    }
  }

  def buildDataFrameFromRDD(rs: JavaRDD[Array[Object]], schema: StructType): DataFrame = {
    spark.createDataFrame(rs.map(Row.fromSeq(_)), schema)
  }

  private def fromJavaSQLType(colType: Int, precision: Int, scale: Int): DataType = colType match {
    case java.sql.Types.BOOLEAN | java.sql.Types.BIT => BooleanType
    case java.sql.Types.TINYINT | java.sql.Types.SMALLINT | java.sql.Types.INTEGER => IntegerType
    case java.sql.Types.BIGINT => LongType
    case java.sql.Types.NUMERIC | java.sql.Types.DECIMAL => DecimalType(precision, scale)
    case java.sql.Types.FLOAT | java.sql.Types.REAL => FloatType
    case java.sql.Types.DOUBLE => DoubleType
    case java.sql.Types.BINARY | java.sql.Types.VARBINARY | java.sql.Types.LONGVARBINARY =>
      BinaryType
    case java.sql.Types.DATE => DateType
    case java.sql.Types.TIMESTAMP => TimestampType
    case _ => StringType
  }

}
