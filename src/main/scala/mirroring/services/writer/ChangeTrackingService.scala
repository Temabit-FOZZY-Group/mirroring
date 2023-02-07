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
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import mirroring.services.SparkService.spark
import wvlet.log.LogSupport
import mirroring.Config

class ChangeTrackingService(
    context: WriterContext
) extends DeltaService(context)
    with LogSupport {

  private val deleteCondition =
    s"${Config.SourceAlias}.SYS_CHANGE_OPERATION = 'D'"
  private val updateCondition = "true"
  private val insertCondition =
    s"${Config.SourceAlias}.SYS_CHANGE_OPERATION in ('I', 'U')"
  private val sourceColPrefix = "SYS_CHANGE_PK_"
  private val excludeColumns  = context.primaryKey.map(col => s"$sourceColPrefix$col")

  override def write(data: DataFrame): Unit = {
    if (DeltaTable.isDeltaTable(spark, context.path)) {

      logger.info("Target table already exists. Merging data...")

      // filter PK columns with sourceColPrefix and make a map for merge
      val columns = data.columns.filterNot(excludeColumns.contains(_))
      val columns_map: Map[String, String] =
        (columns zip columns.map(col => s"source.`$col`")).toMap

      spark.conf.set(
        "spark.databricks.delta.commitInfo.userMetadata",
        context.ctCurrentVersion
      )

      val condition = FilterBuilder.buildMergeCondition(
        context.primaryKey,
        sourceColPrefix = sourceColPrefix,
        ds = data,
        partitionCol = context.lastPartitionCol
      )

      DeltaTable
        .forPath(spark, context.path)
        .as(Config.TargetAlias)
        .merge(data.as(Config.SourceAlias), condition)
        .whenMatched(s"$deleteCondition")
        .delete()
        .whenMatched(s"$updateCondition")
        .updateExpr(columns_map)
        .whenNotMatched(s"$insertCondition")
        .insertExpr(columns_map)
        .execute()

    } else {
      logger.info("Target table doesn't exist yet. Initializing table...")
      super.write(data)
    }
  }

  override def dfWriter(data: DataFrame): DataFrameWriter[Row] = {
    super.dfWriter(data)
  }
}
