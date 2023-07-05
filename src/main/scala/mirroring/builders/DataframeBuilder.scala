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

import mirroring.config.Config
import mirroring.services.SparkContextTrait
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, to_date, to_utc_timestamp}

class DataframeBuilder extends Serializable {
  this: SparkContextTrait =>

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

    var df = jdbcDF
    if (ctx.generateColumn) {
      df = addGeneratedColumn(df, ctx)
    }
    df = renameColumns(df)
    df = castColumnTypes(df, ctx)
    if (!ctx.disablePlatformIngestedAt) {
      // Generate _platform_ingested_at column
      df = df
        .withColumn("_platform_ingested_at", current_timestamp())
        .select("_platform_ingested_at", df.columns: _*)
    }

    df
  }

  private def addGeneratedColumn(jdbcDF: DataFrame, ctx: DataframeBuilderContext): DataFrame = {
    jdbcDF.createOrReplaceTempView(s"${ctx.targetTableName}_tempView")
    getSparkSession
      .sql(
        s"select *, ${ctx.generatedColumnExp} as" +
          s" ${ctx.generatedColumnName} from ${ctx.targetTableName}_tempView"
      )
      .withColumn(
        ctx.generatedColumnName,
        col(ctx.generatedColumnName).cast(ctx.generatedColumnType)
      )
  }

  private def castColumnTypes(df: DataFrame, ctx: DataframeBuilderContext): DataFrame = {
    var res = df
    for (column <- res.columns) {
      if (res.schema(column).dataType.simpleString.contains("date")) {
        res = res.withColumn(column, col(column).cast("timestamp"))
      } else if (res.schema(column).dataType.simpleString.contains("timestamp")) {
        res = res.withColumn(column, to_utc_timestamp(col(column), ctx.timezone))
      }
    }

    if (ctx.writePartitioned) {
      for (
        partitionColumn <- ctx.partitionColumns
          .map(x => FilterBuilder.buildStrWithoutSpecChars(x, "__"))
      ) {
        var resultColumn = col(partitionColumn)
        if (
          res.dtypes.exists(x =>
            x._1.equalsIgnoreCase(partitionColumn) &&
              x._2.equals(Config.SparkTimestampTypeCheck)
          )
        ) {
          resultColumn = to_date(col(partitionColumn))
        }
        res = res.withColumn(partitionColumn, resultColumn)
      }
    }
    res
  }
}

object DataframeBuilder {
  def apply(): DataframeBuilder = new DataframeBuilder() with SparkContextTrait
}
