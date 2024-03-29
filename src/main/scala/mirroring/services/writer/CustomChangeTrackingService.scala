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
import mirroring.config.{Config, UserMetadata}
import mirroring.services.SparkContextTrait
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import wvlet.airframe.codec.MessageCodec
import wvlet.log.Logger

class CustomChangeTrackingService(
    context: WriterContext
) extends DeltaService(context) {
  this: SparkContextTrait =>

  override val logger: Logger = Logger.of[CustomChangeTrackingService]

  private val deleteCondition =
    s"${Config.SourceAlias}.SYS_CHANGE_OPERATION = 'D'"
  private val updateCondition = "true"
  private val insertCondition =
    s"${Config.SourceAlias}.SYS_CHANGE_OPERATION in ('I', 'U')"
  private val sourceColPrefix = "SYS_CHANGE_PK_"
  private val keysCombined = Array
    .concat(
      context.primaryKey,
      context.parentKey
    )
    .distinct
  private val excludeColumns =
    keysCombined.map(col => s"$sourceColPrefix$col") :+ "SYS_CHANGE_OPERATION"
  val lastVersionUserMetadataJSON: String = generateUserMetadataJSON(context.ctLastVersion)
  val userMetadataJSON: String            = generateUserMetadataJSON(context.ctCurrentVersion)

  override def write(data: DataFrame): Unit = {
    if (!DeltaTable.isDeltaTable(getSparkSession, context.path)) {
      logger.info("Target table doesn't exist yet. Initializing table...")
      super.write(data)
    } else {
      logger.info("Target table already exists. Merging data...")
      mergeData(data)
    }
  }

  private def mergeData(data: DataFrame): Unit = {
    verifySchemaMatch(data)

    val updOperations = data.as(Config.SourceAlias).where(insertCondition)
    val deleteOperations = data
      .as(Config.SourceAlias)
      .where(deleteCondition)

    if (updOperations.isEmpty) {
      this.getSparkSession.conf.set(
        "spark.databricks.delta.commitInfo.userMetadata",
        userMetadataJSON
      )
      deleteRows(deleteOperations)
    } else {
      this.getSparkSession.conf.set(
        "spark.databricks.delta.commitInfo.userMetadata",
        lastVersionUserMetadataJSON
      )
      deleteRows(deleteOperations)
      this.getSparkSession.conf.set(
        "spark.databricks.delta.commitInfo.userMetadata",
        userMetadataJSON
      )
      upsertRows(updOperations)
    }
  }

  private def upsertRows(updOperations: DataFrame): Unit = {
    val condition = FilterBuilder.buildMergeCondition(
      context.primaryKey,
      sourceColPrefix = sourceColPrefix,
      ds = updOperations,
      partitionCol = context.lastPartitionCol,
      partitionColPrefix =
        if (context.parentKey.contains(context.lastPartitionCol)) sourceColPrefix else ""
    )

    DeltaTable
      .forPath(this.getSparkSession, context.path)
      .as(Config.TargetAlias)
      .merge(updOperations.as(Config.SourceAlias), condition)
      .whenMatched(s"$deleteCondition")
      .delete()
      .whenMatched(s"$updateCondition")
      .updateAll()
      .whenNotMatched(s"$insertCondition")
      .insertAll()
      .execute()
  }

  override def dfWriter(data: DataFrame): DataFrameWriter[Row] = {
    super.dfWriter(data).option("userMetadata", userMetadataJSON)
  }

  override def verifySchemaMatch(data: DataFrame): Unit = {
    // filter PK columns with sourceColPrefix
    val columns                    = data.columns.filterNot(excludeColumns.contains(_))
    val columnsSource: Set[String] = columns.toSet
    checkSchema(columnsSource)
  }

  private def generateUserMetadataJSON(ctCurrentVersion: BigInt): String = {
    val userMetadata             = UserMetadata(ctCurrentVersion)
    val codec                    = MessageCodec.of[UserMetadata]
    val userMetadataJSON: String = codec.toJson(userMetadata)
    userMetadataJSON
  }

  override def getUserMetadataJSON: String = {
    userMetadataJSON
  }

  private def deleteRows(deleteOperations: DataFrame): Unit = {
    val condition = FilterBuilder.buildMergeCondition(
      context.parentKey,
      sourceColPrefix = sourceColPrefix,
      ds = deleteOperations,
      partitionCol = context.lastPartitionCol,
      partitionColPrefix =
        if (context.parentKey.contains(context.lastPartitionCol)) sourceColPrefix else ""
    )

    DeltaTable
      .forPath(this.getSparkSession, context.path)
      .as(Config.TargetAlias)
      .merge(
        deleteOperations.as(Config.SourceAlias),
        condition
      )
      .whenMatched()
      .delete()
      .execute()
  }
}
