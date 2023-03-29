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

case class WriterContext(
    private val _mode: String,
    private val _pathToSave: String,
    private val _partitionCols: Array[String],
    private val _lastPartitionCol: String,
    private val _mergeKeys: Array[String],
    private val _primaryKey: Array[String],
    private val _whereClause: String,
    private val _hiveDb: String,
    private val _targetTableName: String,
    private val _ct_debug: Boolean
) {

  private var _ctCurrentVersion: String = ""

  def ctCurrentVersion: String = _ctCurrentVersion

  def ctCurrentVersion_=(version: BigInt): Unit = _ctCurrentVersion = version.toString

  val mode: String                 = _mode
  val path: String                 = _pathToSave
  val partitionCols: Array[String] = _partitionCols
  val lastPartitionCol: String     = _lastPartitionCol
  val mergeKeys: Array[String]     = _mergeKeys
  val primaryKey: Array[String]    = _primaryKey
  val whereClause: String          = _whereClause
  val hiveDb: String               = _hiveDb
  val targetTableName: String      = _targetTableName
  val ct_debug: Boolean            = _ct_debug

}
