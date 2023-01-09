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

package palefat.data.mirror

import palefat.data.mirror.builders.{DataframeBuilderContext, FilterBuilder, SqlBuilder}
import palefat.data.mirror.services.databases.JdbcContext
import palefat.data.mirror.services.writer._
import wvlet.log.LogSupport

import java.time.LocalDate
import scala.collection.mutable

object Config {
  val SparkTimestampTypeCheck: String = "TimestampType"
  val TargetAlias: String             = "target"
  val SourceAlias: String             = "source"
  val Timezone: String                = "Europe/Kiev"
}

case class Config(
    private val _pathToSave: String,
    schema: String,
    tab: String,
    private val _whereClause: String,
    private val _query: String,
    private val _jdbcUrl: String,
    var mode: String,
    numPart: String,
    splitBy: String,
    private val _calcMinDt: String,
    private val _calcMaxDt: String,
    dtFlt: String,
    private val _execDate: String,
    writePartitioned: Boolean,
    private val _partitionCol: String,
    useMerge: Boolean,
    private val _mergeKeys: String,
    hiveDb: String,
    hiveDbLocation: String,
    generateColumn: Boolean,
    generatedColumnName: String,
    generatedColumnExp: String,
    generatedColumnType: String,
    forcePartition: Boolean,
    timezone: String,
    isChangeTrackingEnabled: Boolean,
    private val _primaryKey: String,
    private val _zorderbyCol: String,
    logLvl: String,
    logSparkLvl: String
) extends LogSupport {

  FlowLogger.init(schema, tab, logLvl)
  var minDate: LocalDate = _
  var maxDate: LocalDate = _

  if (_calcMinDt.nonEmpty && _calcMaxDt.nonEmpty) {
    minDate = LocalDate.parse(_calcMinDt)
    maxDate = LocalDate.parse(_calcMaxDt)
  } else if (_execDate.nonEmpty) {
    minDate = LocalDate.parse(_execDate)
    maxDate = LocalDate.parse(_execDate).plusDays(1)
  }

  val targetTableName: String =
    s"${FilterBuilder.buildStrWithoutSpecChars(schema).toLowerCase}__${FilterBuilder.buildStrWithoutSpecChars(tab).toLowerCase}"
  val pathToSave: String           = s"${_pathToSave}/$targetTableName"
  val mergeKeys: Array[String]     = stringToArray(_mergeKeys)
  val primary_key: Array[String]   = stringToArray(_primaryKey)
  val zorderby_col: Array[String]  = stringToArray(_zorderbyCol)
  val partitionCols: Array[String] = stringToArray(_partitionCol)
  val lastPartitionCol: String =
    if (partitionCols.length > 0) partitionCols.last else ""

  def stringToArray(in: String): Array[String] = {
    in.replace("]", "")
      .replace("[", "")
      .replace("'", "")
      .split(",")
      .map(x => x.trim)
      .filter(x => x.nonEmpty)
  }

  require(_pathToSave.nonEmpty, "Parameter `path_to_save` is required.")

  require(tab.nonEmpty, "Parameter `tab` is required.")

  require(_jdbcUrl.nonEmpty, "Parameter `jdbcUrl` is required.")

  require(
    !(dtFlt.nonEmpty ^ (_execDate.nonEmpty || (_calcMinDt.nonEmpty && _calcMaxDt.nonEmpty))),
    "dtflt has to be used with exec_date or (calc_max_dt, calc_min_dt)."
  )

  require(
    useMerge ^ mergeKeys.length == 0,
    s"Parameter `useMerge` and `mergeKeys` should be both specified."
  )

  require(
    writePartitioned ^ partitionCols.length == 0,
    s"Parameter `writePartitioned` and `partitionCol` should be both specified."
  )

  require(
    (if (dtFlt.nonEmpty && writePartitioned) {
       partitionCols.contains(dtFlt)
     } else { true }) || useMerge || forcePartition,
    s"If `dtflt` is different from `partition_col`, you should `use_merge`. ${partitionCols
      .mkString("=")} _ $dtFlt _ ${partitionCols.contains(dtFlt)}"
  )

  require(
    !(generateColumn ^ generatedColumnName.nonEmpty ^ generatedColumnExp.nonEmpty ^ generatedColumnType.nonEmpty),
    "Parameter `generate_column`, `generated_column_name`, `generated_column_exp` and `generated_column_type` should be all specified."
  )

  require(
    isChangeTrackingEnabled ^ primary_key.length == 0,
    s"Parameter `primary_key` should be specified if `isChangeTrackingEnabled` is true."
  )

  val whereClause = new mutable.StringBuilder("1=1")

  if (dtFlt.nonEmpty) {
    whereClause.append(SqlBuilder.buildBetween(dtFlt, minDate, maxDate))
  }

  if (_whereClause.nonEmpty) {
    whereClause.append(s" AND (${_whereClause})")
  }

  val query: String =
    if (_query.isEmpty) {
      SqlBuilder.buildSelectTableSQL(schema, tab, whereClause.toString)
    } else if (_whereClause.nonEmpty) {
        s"(${_query} where ${whereClause.toString}) as subq"
    } else {
        s"(${_query}) as subq"
    }

  def getDataframeBuilderContext: DataframeBuilderContext = {
    DataframeBuilderContext(
      targetTableName = targetTableName,
      writePartitioned = writePartitioned,
      partitionColumns = partitionCols,
      timezone = timezone,
      generateColumn = generateColumn,
      generatedColumnExp = generatedColumnExp,
      generatedColumnName = generatedColumnName,
      generatedColumnType = generatedColumnType
    )
  }

  def getJdbcContext: JdbcContext = {
    JdbcContext(
      jdbcUrl = _jdbcUrl,
      inTable = tab,
      inSchema = schema,
      numPart = numPart,
      splitby = splitBy
    )
  }

  def getWriterContext: WriterContext = {
    WriterContext(
      mode,
      pathToSave,
      partitionCols,
      lastPartitionCol,
      mergeKeys,
      primary_key,
      whereClause.toString
    )
  }

  override def toString: String = {
    s"""path_to_save - $pathToSave,
       |tab - $tab,
       |targetTableName - $targetTableName,
       |schema - $schema,
       |where - $whereClause,
       |query - $query,
       |mode - $mode,
       |numpart - $numPart,
       |splitby - $splitBy,
       |calc_min_dt - $minDate,
       |calc_max_dt - $maxDate,
       |dtflt - $dtFlt,
       |exec_date - $minDate,
       |write_partitioned - $writePartitioned,
       |partition_col - [${partitionCols.mkString(", ")}],
       |use_merge - $useMerge,
       |merge_keys - [${mergeKeys.mkString(", ")}],
       |hive_db - $hiveDb,
       |hive_db_location - $hiveDbLocation,
       |generate_column - $generateColumn,
       |generated_column_name - $generatedColumnName,
       |generated_column_exp - $generatedColumnExp,
       |generated_column_type - $generatedColumnType,
       |force_partition - $forcePartition,
       |timezone - $timezone,
       |change_tracking - $isChangeTrackingEnabled,
       |primary_key - [${primary_key.mkString(", ")}],
       |zorderby_col - [${zorderby_col.mkString(", ")}],
       |log_lvl - $logLvl,
       |log_spark_lvl - $logSparkLvl
       |""".stripMargin

  }

}
