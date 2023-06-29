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

package mirroring.services

import io.delta.tables.DeltaTable
import mirroring.builders.SqlBuilder
import mirroring.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.implicits.stringEncoder
import wvlet.log.LogSupport

object SqlService extends LogSupport with SparkContextTrait {

  def run(config: Config, userMetadataJSON: String): Unit = {

    val createDbSQL =
      SqlBuilder.buildCreateDbSQL(config.hiveDb, config.hiveDbLocation)
    val createTableSQL = SqlBuilder.buildCreateTableSQL(
      config.hiveDb,
      config.targetTableName,
      config.pathToSave
    )

    val spark = getSparkSession

    if (DeltaTable.isDeltaTable(spark, config.pathToSave)) {
      logger.info(s"Running SQL: $createDbSQL")
      spark.sql(createDbSQL)
      logger.info(s"Running SQL: ${createTableSQL.linesIterator.mkString(" ").trim}")
      spark.sql(createTableSQL)

      val logRetentionDuration         = getLogRetentionDuration(config, spark)
      val deletedFileRetentionDuration = getDeletedFileLogRetention(config, spark)

      if (logRetentionConfigIsChanged(config, logRetentionDuration, deletedFileRetentionDuration)) {
        val alterTableSQL = SqlBuilder.buildAlterTableSQL(
          config.hiveDb,
          config.targetTableName,
          config.logRetentionDuration,
          config.deletedFileRetentionDuration
        )
        if (config.isChangeTrackingEnabled || config.isCustomCTEnabled) {
          spark.conf.set(
            "spark.databricks.delta.commitInfo.userMetadata",
            userMetadataJSON
          )
        }
        spark.sql(alterTableSQL)
      }
    }
  }

  private def logRetentionConfigIsChanged(
      config: Config,
      logRetentionDuration: String,
      deletedFileRetentionDuration: String
  ) = {
    !logRetentionDuration.equals(config.logRetentionDuration) || !deletedFileRetentionDuration
      .equals(config.deletedFileRetentionDuration)
  }

  private def getDeletedFileLogRetention(config: Config, spark: SparkSession) = {
    spark
      .sql(s"""SHOW TBLPROPERTIES ${config.hiveDb}.${config.targetTableName}
           |('delta.deletedFileRetentionDuration');""".stripMargin)
      .select("value")
      .as[String]
      .first
  }

  private def getLogRetentionDuration(config: Config, spark: SparkSession) = {
    spark
      .sql(
        s"SHOW TBLPROPERTIES ${config.hiveDb}.${config.targetTableName} ('delta.logRetentionDuration');"
      )
      .select("value")
      .as[String]
      .first
  }
}
