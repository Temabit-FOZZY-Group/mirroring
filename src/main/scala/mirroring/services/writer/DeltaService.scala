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

package mirroring.services.writer

import io.delta.tables.DeltaTable
import mirroring.builders.FilterBuilder
import mirroring.services.{MirroringManager, SparkContextTrait}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import wvlet.log.{LogSupport, Logger}

class DeltaService(context: WriterContext) {
  this: SparkContextTrait =>

  val logger: Logger = Logger.of[DeltaService]

  def write(data: DataFrame): Unit = {
    if (DeltaTable.isDeltaTable(this.getSparkSession, context.path)) {
      verifySchemaMatch(data)
    }
    logger.info(s"Saving data to ${context.path}")
    dfWriter(data).save(context.path)
    logger.info(s"Saved data to ${context.path}")
  }

  def dfWriter(data: DataFrame): DataFrameWriter[Row] = {
    var writer = data.write
      .mode(context.mode)
      .format("delta")

    val replaceWhere = FilterBuilder.buildReplaceWherePredicate(
      ds = data,
      partitionCol = context.lastPartitionCol,
      whereClause = context.whereClause
    )
    if (replaceWhere.nonEmpty && context.mode == "overwrite") {
      logger.info(
        s"Data matching next condition will be replaced: $replaceWhere"
      )
      writer = writer
        .option("replaceWhere", replaceWhere)
    }
    if (context.partitionCols.nonEmpty) {
      logger.info("Saving data with partition")
      writer = writer
        .partitionBy(context.partitionCols: _*)
    }
    writer
  }

  protected def verifySchemaMatch(data: DataFrame): Unit = {
    val columnsSource: Set[String] = data.columns.toSet
    checkSchema(columnsSource)
  }

  protected def checkSchema(columnsSource: Set[String]): Unit = {
    logger.info("Checking if schema match for source and destination...")
    val df_reader                = this.getSparkSession.read.format("delta")
    val df                       = df_reader.load(context.path)
    val columnsDest: Set[String] = df.columns.toSet
    val columnsDiff: Set[String] = (columnsSource &~ columnsDest) | (columnsDest &~ columnsSource)
    if (columnsDiff.nonEmpty) {
      logger.error(s"Schema columns difference: ${columnsDiff.mkString(", ")}")
      throw new SchemaNotMatchException()
    }
  }

  def getUserMetadataJSON: String = {
    ""
  }
}
