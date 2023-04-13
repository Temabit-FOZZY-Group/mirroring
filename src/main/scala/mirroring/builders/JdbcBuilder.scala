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
  BooleanType,
  DateType,
  DoubleType,
  IntegerType,
  FloatType,
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{DataFrame, Row}
import mirroring.builders.SqlBuilder.buildSQLObjectName
import mirroring.services.SparkService.spark
import mirroring.services.databases.JdbcContext
import wvlet.log.LogSupport

object JdbcBuilder extends LogSupport {

  def buildJDBCResultSet(
      connection: Connection,
      query: String,
      parameters: Array[String] = Array[String]()
  ): ResultSet = {
    val cStmt: CallableStatement =
      connection.prepareCall(query)
    for ((parameter, i) <- parameters.zipWithIndex) {
      cStmt.setString(i + 1, parameter)
    }
    cStmt.execute()
    val rs: ResultSet = cStmt.getResultSet
    rs
  }

  def buildStructFromResultSet(rs: ResultSet): StructType = {
    val md          = rs.getMetaData
    val columnCount = md.getColumnCount
    val structFieldsList: List[StructField] = (1 to columnCount).map { i =>
      StructField(
        md.getColumnName(i),
        md.getColumnType(i) match {
          case java.sql.Types.BOOLEAN   => BooleanType
          case java.sql.Types.INTEGER   => IntegerType
          case java.sql.Types.BIGINT    => LongType
          case java.sql.Types.DOUBLE    => DoubleType
          case java.sql.Types.FLOAT     => FloatType
          case java.sql.Types.DATE      => DateType
          case java.sql.Types.TIMESTAMP => TimestampType
          case _                        => StringType
        },
        md.isNullable(i) == 1
      )
    }.toList
    StructType(structFieldsList)
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

  def buildDataFrameFromRDD(rs: JdbcRDD[Array[Object]], schema: StructType): DataFrame = {
    spark.createDataFrame(rs.map(Row.fromSeq(_)), schema)
  }

}
