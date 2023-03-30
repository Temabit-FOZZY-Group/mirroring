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

import org.apache.spark.sql.{DataFrame, Encoders}
import wvlet.log.LogSupport
import mirroring.Config
import scala.collection.mutable

object FilterBuilder extends LogSupport {

  def buildJoinCondition(
      keys: Array[String],
      source: String,
      target: String
  ): String = {
    keys
      .map(x => s"$source.${x.trim} = $target.${x.trim}")
      .reduce(_ + " and " + _)
  }

  def buildMergeCondition(
      primaryKey: Array[String],
      ds: DataFrame,
      partitionCol: String = "",
      sourceColPrefix: String = "",
      targetColPrefix: String = "",
      partitionColPrefix: String = ""
  ): String = {
    val conditions: mutable.ArrayBuilder[String] = Array.newBuilder[String]
    lazy val partitionFilter =
      FilterBuilder.buildReplaceWherePredicate(
        ds = ds,
        partitionCol = partitionCol,
        partitionColPrefix = partitionColPrefix,
        partitionColTargetSchema = Config.TargetAlias
      )
    if (partitionCol.nonEmpty && partitionFilter.nonEmpty) {
      conditions += partitionFilter
    }
    conditions += primaryKey
      .map(colName =>
        s"${Config.SourceAlias}.$sourceColPrefix${colName.trim} = ${Config.TargetAlias}.$targetColPrefix${colName.trim}"
      )
      .mkString(" and ")
    val condition =
      conditions.result.filter(x => x.nonEmpty).reduce(_ + " and " + _)
    logger.info(s"Data will be merged using next condition: $condition")
    condition
  }

  def buildStrWithoutSpecChars(
      in: String,
      replacement: String = "_"
  ): String = {
    in.replaceAll("\\W", replacement)
  }

  def buildReplaceWherePredicate(
      ds: DataFrame,
      partitionCol: String,
      partitionColPrefix: String = "",
      partitionColTargetSchema: String = "",
      whereClause: String = ""
  ): String = {
    if (ds != null && partitionCol.nonEmpty) {
      val replaceWhere = new mutable.StringBuilder(s"$whereClause")
      if (whereClause.nonEmpty) {
        replaceWhere.append(" AND ")
      }

      val values = ds
        .select(s"$partitionColPrefix$partitionCol")
        .distinct
        .as[String](Encoders.STRING)
        .filter(x => !x.toLowerCase.contains("null"))
        .cache()

      if (!values.isEmpty) {
        replaceWhere.append(s"$partitionColTargetSchema.$partitionCol in (")
        replaceWhere.append(
          values
            .map(partition =>
              if (
                !partition
                  .forall(_.isDigit) && partition != "true" && partition != "false"
              ) {
                s"'$partition'"
              } else {
                partition
              }
            )(Encoders.STRING)
            .reduce(_ + ", " + _)
        )
        replaceWhere.append(")")
      }
      replaceWhere.toString
    } else {
      whereClause
    }
  }

}
