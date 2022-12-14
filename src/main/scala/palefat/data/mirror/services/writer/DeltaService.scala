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

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import wvlet.log.LogSupport

class DeltaService(context: WriterContext) extends LogSupport {

  def write(data: DataFrame): Unit = {
    logger.info(s"Saving data to ${context.path}")
    dfWriter(data).save(context.path)
  }

  def dfWriter(data: DataFrame): DataFrameWriter[Row] = {
    var writer = data.write
      .mode(context.mode)
      .format("delta")
      .option("mergeSchema", "true")
      .option("userMetadata", context.ctCurrentVersion)

    if (context.partitionCols.nonEmpty) {
      logger.info("Saving data with partition")

      val replaceWhere = context.whereClause.replaceFirst("1=1 AND ", "")
      logger.info(
        s"Data matching next condition will be replaced: $replaceWhere"
      )

      writer = writer
        .partitionBy(context.partitionCols: _*)
        .option("replaceWhere", replaceWhere)
        .mode("overwrite")
    }
    writer
  }
}
