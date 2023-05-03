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
import mirroring.Config
import mirroring.builders.FilterBuilder
import mirroring.services.SparkService.spark
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import wvlet.log.LogSupport

class MergeService(context: WriterContext) extends DeltaService(context) with LogSupport {

  override def write(data: DataFrame): Unit = {
    if (DeltaTable.isDeltaTable(spark, context.path)) {
      logger.info("Target table already exists. Merging data...")

      verifySchemaMatch(data)

      if (context.lastPartitionCol.nonEmpty) {
        spark.conf.set(
          "spark.databricks.delta.merge.repartitionBeforeWrite.enabled",
          "true"
        )
      }
      DeltaTable
        .forPath(spark, context.path)
        .as(Config.TargetAlias)
        .merge(
          data.as(Config.SourceAlias),
          FilterBuilder.buildMergeCondition(
            context.mergeKeys,
            data,
            context.lastPartitionCol
          )
        )
        .whenMatched
        .updateAll()
        .whenNotMatched
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
