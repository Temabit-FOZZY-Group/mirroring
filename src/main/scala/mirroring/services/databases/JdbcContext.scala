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

package mirroring.services.databases

case class JdbcContext(
    private val jdbcUrl: String,
    private val inTable: String,
    private val inSchema: String,
    private val numPart: String,
    private val splitby: String,
    var _changeTrackingLastVersion: () => Option[BigInt],
    var _ctCurrentVersion: Option[BigInt] = None,
    private val _CTChangesQuery: String,
    private val _CTChangesQueryParams: Array[String]
) {
  val url: String                         = jdbcUrl
  val table: String                       = inTable
  val schema: String                      = inSchema
  val partitionColumn: String             = splitby
  val numPartitions: String               = numPart
  lazy val ctLastVersion: BigInt          = _changeTrackingLastVersion().get
  lazy val ctCurrentVersion: BigInt       = _ctCurrentVersion.get
  val ctChangesQuery: String              = _CTChangesQuery
  val ctChangesQueryParams: Array[String] = _CTChangesQueryParams
}
