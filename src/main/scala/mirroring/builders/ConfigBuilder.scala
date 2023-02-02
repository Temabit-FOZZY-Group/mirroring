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

import mirroring.Config

object ConfigBuilder {

  private val mapArgsDefault = scala.collection.mutable.Map[String, String](
    "path_to_save"                 -> "",
    "tab"                          -> "",
    "schema"                       -> "dbo",
    "where"                        -> "",
    "query"                        -> "",
    "jdbcUrl"                      -> "",
    "mode"                         -> "errorifexists",
    "numpart"                      -> "",
    "splitby"                      -> "",
    "calc_min_dt"                  -> "",
    "calc_max_dt"                  -> "",
    "dtflt"                        -> "",
    "exec_date"                    -> "",
    "write_partitioned"            -> "false",
    "partition_col"                -> "",
    "use_merge"                    -> "false",
    "merge_keys"                   -> "",
    "hive_db"                      -> "",
    "hive_db_location"             -> "s3a://warehouse/",
    "generate_column"              -> "false",
    "generated_column_name"        -> "",
    "generated_column_exp"         -> "",
    "generated_column_type"        -> "",
    "timezone"                     -> Config.Timezone,
    "force_partition"              -> "false",
    "change_tracking"              -> "false",
    "primary_key"                  -> "",
    "zorderby_col"                 -> "",
    "log_lvl"                      -> "info",
    "log_spark_lvl"                -> "WARN",
    "ct_changes_query"             -> "",
    "ct_changes_query_params"      -> "",
    "ct_min_valid_version_query"   -> "",
    "ct_min_valid_version_params"  -> "",
    "ct_current_version_query"     -> "",
    "ct_current_version_params"    -> "",
    "disable_platform_ingested_at" -> "false"
  )

  def parse(
      arguments: Array[String]
  ): scala.collection.mutable.Map[String, String] = {
    val mapArgs = mapArgsDefault.clone()
    arguments.foreach { arg =>
      val key   = arg.split("==")(0)
      val value = arg.split("==")(1)
      if (value != "None") {
        mapArgs.update(key, value)
      }
    }
    mapArgs
  }

  def build(arguments: scala.collection.mutable.Map[String, String]): Config = {
    Config(
      _pathToSave = arguments("path_to_save"),
      tab = arguments("tab"),
      schema = arguments("schema"),
      _whereClause = arguments("where"),
      _query = arguments("query"),
      _jdbcUrl = arguments("jdbcUrl"),
      mode = arguments("mode"),
      numPart = arguments("numpart"),
      splitBy = arguments("splitby"),
      _calcMinDt = arguments("calc_min_dt"),
      _calcMaxDt = arguments("calc_max_dt"),
      dtFlt = arguments("dtflt"),
      _execDate = arguments("exec_date"),
      writePartitioned = arguments("write_partitioned").toBoolean,
      _partitionCol = arguments("partition_col"),
      useMerge = arguments("use_merge").toBoolean,
      _mergeKeys = arguments("merge_keys"),
      hiveDb = arguments("hive_db"),
      hiveDbLocation = arguments("hive_db_location"),
      generateColumn = arguments("generate_column").toBoolean,
      generatedColumnName = arguments("generated_column_name"),
      generatedColumnExp = arguments("generated_column_exp"),
      generatedColumnType = arguments("generated_column_type"),
      forcePartition = arguments("force_partition").toBoolean,
      timezone = arguments("timezone"),
      isChangeTrackingEnabled = arguments("change_tracking").toBoolean,
      _primaryKey = arguments("primary_key"),
      _zorderbyCol = arguments("zorderby_col"),
      logLvl = arguments("log_lvl"),
      logSparkLvl = arguments("log_spark_lvl"),
      CTChangesQuery = arguments("ct_changes_query"),
      _CTChangesQueryParams = arguments("ct_changes_query_params"),
      CTMinValidVersionQuery = arguments("ct_min_valid_version_query"),
      _CTMinValidVersionParams = arguments("ct_min_valid_version_params"),
      CTCurrentVersionQuery = arguments("ct_current_version_query"),
      _CTCurrentVersionParams = arguments("ct_current_version_params"),
      disablePlatformIngestedAt = arguments("disable_platform_ingested_at").toBoolean
    )
  }

}
