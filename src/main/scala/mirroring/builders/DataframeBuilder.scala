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

package mirroring.builders

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date, to_utc_timestamp, current_timestamp}
import mirroring.services.SparkService
import mirroring.Config

object DataframeBuilder {

  def renameColumns(jdbcDF: DataFrame): DataFrame = {
    jdbcDF.columns.foldLeft(jdbcDF)((curr, n) =>
      curr
        .withColumnRenamed(
          n,
          n.replaceAll("\\s", "__")
            .replaceAll("\\(|\\)", "")
            .replaceAll("\\.", "__")
            .replaceAll(":", "__")
        )
    )
  }

  def buildDataFrame(
      jdbcDF: DataFrame,
      ctx: DataframeBuilderContext
  ): DataFrame = {

    val spark = SparkService.spark
    var df    = jdbcDF

    if (ctx.generateColumn) {
      jdbcDF.createOrReplaceTempView(s"${ctx.targetTableName}_tempView")
      val generatedDs = spark.sql(
        s"select *, ${ctx.generatedColumnExp} as" +
          s" ${ctx.generatedColumnName} from ${ctx.targetTableName}_tempView"
      )
      df = generatedDs.withColumn(
        ctx.generatedColumnName,
        col(ctx.generatedColumnName).cast(ctx.generatedColumnType)
      )
    }

    df = renameColumns(df)
    for (column <- df.columns) {
      if (df.schema(column).dataType.simpleString.contains("date")) {
        df = df.withColumn(column, col(column).cast("timestamp"))
      } else if (df.schema(column).dataType.simpleString.contains("timestamp")) {
        df = df.withColumn(column, to_utc_timestamp(col(column), ctx.timezone))
      }
    }

    if (ctx.writePartitioned) {
      for (
        partitionColumn <- ctx.partitionColumns
          .map(x => FilterBuilder.buildStrWithoutSpecChars(x, "__"))
      ) {
        var resultColumn = col(partitionColumn)
        if (
          df.dtypes.exists(x =>
            x._1.equalsIgnoreCase(partitionColumn) &&
              x._2.equals(Config.SparkTimestampTypeCheck)
          )
        ) {
          resultColumn = to_date(col(partitionColumn))
        }
        df = df.withColumn(partitionColumn, resultColumn)
      }
    }

    // Generate _platform_ingested_at column
    df = df.withColumn("_platform_ingested_at", current_timestamp())

    df
  }
}
