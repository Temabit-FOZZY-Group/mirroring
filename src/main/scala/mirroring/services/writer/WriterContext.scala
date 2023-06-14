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
    private val _parentKey: Array[String],
    private val _whereClause: String,
    var _ctCurrentVersion: Option[BigInt] = None
) {

  val mode: String                  = _mode
  val path: String                  = _pathToSave
  val partitionCols: Array[String]  = _partitionCols
  val lastPartitionCol: String      = _lastPartitionCol
  val mergeKeys: Array[String]      = _mergeKeys
  val primaryKey: Array[String]     = _primaryKey
  val parentKey: Array[String]     = _parentKey
  val whereClause: String           = _whereClause
  lazy val ctCurrentVersion: BigInt = _ctCurrentVersion.get

}
