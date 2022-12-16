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

package palefat.data.mirror.services.writer

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import palefat.data.mirror.Config
import palefat.data.mirror.builders.FilterBuilder
import palefat.data.mirror.services.SparkService.spark
import wvlet.log.LogSupport

class ChangeTrackingService(
    context: WriterContext
) extends DeltaService(context)
    with LogSupport {

  private val deleteCondition =
    s"${Config.SourceAlias}.SYS_CHANGE_OPERATION = 'D'"
  private val updateCondition = "true"
  private val insertCondition =
    s"${Config.SourceAlias}.SYS_CHANGE_OPERATION in ('I', 'U')"

  override def write(data: DataFrame): Unit = {
    if (DeltaTable.isDeltaTable(spark, context.path)) {

      logger.info("Target table already exists. Merging data...")

      spark.conf.set(
        "spark.databricks.delta.commitInfo.userMetadata",
        context.ctCurrentVersion
      )

      val condition = FilterBuilder.buildMergeCondition(
        context.primaryKey,
        sourceColPrefix = "SYS_CHANGE_PK_",
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
        .updateAll()
        .whenNotMatched(s"$insertCondition")
        .insertAll()
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
