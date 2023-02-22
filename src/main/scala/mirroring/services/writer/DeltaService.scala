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

import mirroring.builders.FilterBuilder
import mirroring.services.SparkService.spark
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import wvlet.log.LogSupport

class DeltaService(context: WriterContext) extends LogSupport {

  def write(data: DataFrame): Unit = {
    logger.info(s"Saving data to ${context.path}")
    dfWriter(data).save(context.path)
    logger.info(s"Saved data to ${context.path}")
  }

  def dfWriter(data: DataFrame): DataFrameWriter[Row] = {
    var writer = data.write
      .mode(context.mode)
      .format("delta")
      .option("userMetadata", context.ctCurrentVersion)

    val replaceWhere = FilterBuilder.buildReplaceWherePredicate(
      data,
      context.lastPartitionCol,
      context.whereClause
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

  protected def checkSchema(columnsSource: Set[String]): Unit = {
    logger.info("Checking if schema match for source and destination...")
    val df_reader                = spark.read.format("delta")
    val df                       = df_reader.load(context.path)
    val columnsDest: Set[String] = df.columns.toSet
    val columnsDiff: Set[String] = (columnsSource &~ columnsDest) | (columnsDest &~ columnsSource)
    if (columnsDiff.nonEmpty) {
      logger.error(s"Schema columns difference: ${columnsDiff.mkString(", ")}")
      throw new SchemaNotMatchException()
    }
  }
}
