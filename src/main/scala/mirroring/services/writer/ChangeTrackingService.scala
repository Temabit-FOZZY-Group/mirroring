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
import mirroring.builders.{FilterBuilder, SqlBuilder}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import mirroring.services.SparkService.spark
import org.apache.spark.sql.functions.lit
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
  private val excludeColumns =
    context.primaryKey.map(col => s"$sourceColPrefix$col") :+ "SYS_CHANGE_OPERATION"

  override def write(data: DataFrame): Unit = {
    if (DeltaTable.isDeltaTable(spark, context.path)) {
      logger.info("Target table already exists. Merging data...")
      verifySchemaMatch(data)

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
        .updateAll()
        .whenNotMatched(s"$insertCondition")
        .insertAll()
        .execute()

    } else {
      logger.info("Target table doesn't exist yet. Initializing table...")
      super.write(data)
    }
    if (context.ct_debug) {
      writeCtDebug(data)
    }
  }

  override def dfWriter(data: DataFrame): DataFrameWriter[Row] = {
    super.dfWriter(data)
  }

  override def verifySchemaMatch(data: DataFrame): Unit = {
    // filter PK columns with sourceColPrefix
    val columns                    = data.columns.filterNot(excludeColumns.contains(_))
    val columnsSource: Set[String] = columns.toSet
    checkSchema(columnsSource)
  }

  private def writeCtDebug(data: DataFrame): Unit = {
    val pathCtDebug: String = s"${context.path}_ct_debug"
    logger.info(s"Writing CT data as is into $pathCtDebug...")
    val contextCtDebug: WriterContext = WriterContext(
      _mode = "append",
      _pathToSave = pathCtDebug,
      _partitionCols = context.partitionCols,
      _lastPartitionCol = context.lastPartitionCol,
      _mergeKeys = context.mergeKeys,
      _primaryKey = context.primaryKey,
      _whereClause = context.whereClause,
      _hiveDb = "",
      _targetTableName = "",
      _ct_debug = false
    )
    val ctDebugData                        = data.withColumn("ctCurrentVersion", lit(context.ctCurrentVersion))
    val ctDebugWriterService: DeltaService = new DeltaService(contextCtDebug)
    ctDebugWriterService.write(ctDebugData)
    val createTableSQL = SqlBuilder.buildCreateTableSQL(
      context.hiveDb,
      s"${context.targetTableName}_ct_debug",
      pathCtDebug
    )
    logger.info(s"Running SQL: $createTableSQL")
    spark.sql(createTableSQL)
  }
}
