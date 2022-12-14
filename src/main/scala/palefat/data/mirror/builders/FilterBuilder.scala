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

package palefat.data.mirror.builders

import org.apache.spark.sql.{DataFrame, Encoders}
import palefat.data.mirror.Config
import wvlet.log.LogSupport

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
      targetColPrefix: String = ""
  ): String = {
    val conditions: mutable.ArrayBuilder[String] = Array.newBuilder[String]
    lazy val partitionFilter =
      FilterBuilder.buildReplaceWherePredicate(ds, partitionCol)
    if (partitionCol.nonEmpty) {
      conditions += partitionFilter.replace(
        partitionCol,
        s"${Config.TargetAlias}.$partitionCol"
      )
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
      partitionCol: String
  ): String = {
    if (ds != null && partitionCol.nonEmpty && !ds.isEmpty) {
      val replaceWhere = new mutable.StringBuilder
      replaceWhere.append(s"$partitionCol in (")
      val values = ds
        .select(partitionCol)
        .distinct
        .as[String](Encoders.STRING)
        .filter(x => !x.toLowerCase.contains("null"))
        .map(partition =>
          if (
            !partition
              .forall(_.isDigit) && partition != "true" && partition != "false"
          )
            s"'$partition'"
          else
            partition
        )(Encoders.STRING)
        .reduce(_ + ", " + _)
      replaceWhere.append(values)
      replaceWhere.append(")")
      replaceWhere.toString
    } else {
      ""
    }
  }

}
