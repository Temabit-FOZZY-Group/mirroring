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

package palefat.data.mirror.services

import io.delta.tables.DeltaTable
import palefat.data.mirror.Config
import palefat.data.mirror.builders.SqlBuilder
import SparkService.spark
import wvlet.log.LogSupport

object SqlService extends LogSupport {

  def run(config: Config): Unit = {

    val createDbSQL =
      SqlBuilder.buildCreateDbSQL(config.hiveDb, config.hiveDbLocation)
    val dropTableSQL =
      SqlBuilder.buildDropTableSQL(config.hiveDb, config.targetTableName)
    val createTableSQL = SqlBuilder.buildCreateTableSQL(
      config.hiveDb,
      config.targetTableName,
      config.pathToSave
    )

    if (DeltaTable.isDeltaTable(spark, config.pathToSave)) {
      logger.info(s"Running SQL: $createDbSQL")
      spark.sql(createDbSQL)
      logger.info(s"Running SQL: $dropTableSQL")
      spark.sql(dropTableSQL)
      logger.info(s"Running SQL: $createTableSQL")
      spark.sql(createTableSQL)
    }
  }
}
