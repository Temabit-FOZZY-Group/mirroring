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

package palefat.data.mirror.services

import io.delta.tables.DeltaTable
import SparkService.spark
import wvlet.log.LogSupport

object DeltaTableService extends LogSupport {

  def runVacuum(pathToSave: String): Unit = {
    if (DeltaTable.isDeltaTable(spark, pathToSave)) {
      DeltaTable.forPath(spark, pathToSave).vacuum()
    }
  }

  def executeZOrdering(
      pathToSave: String,
      zorderbyCol: Array[String],
      whereClause: String = "1=1"
  ): Unit = {
    val predicate = if (whereClause.isEmpty) "1=1" else whereClause

    logger.info(s"""Running Z-Ordering for column(s) [${zorderbyCol.mkString(
      ","
    )}] where [$predicate]""")
    DeltaTable
      .forPath(spark, pathToSave)
      .optimize()
      .where(predicate)
      .executeZOrderBy(zorderbyCol: _*)
    logger.info("Finished z-ordering")
  }

}
