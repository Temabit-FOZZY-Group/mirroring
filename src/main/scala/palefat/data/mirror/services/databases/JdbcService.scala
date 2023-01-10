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

package palefat.data.mirror.services.databases

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import palefat.data.mirror.services.SparkService.spark
import wvlet.log.LogSupport

class JdbcService(jdbcContext: JdbcContext) extends BaseJdbcService(jdbcContext) with LogSupport {

  def loadData(_query: String): DataFrame = {
    logger.info(s"Reading data with query: ${_query}")
    dfReader.option("dbtable", _query).load()
  }

  def dfReader: DataFrameReader = {
    spark.read
      .format("jdbc")
      .option("url", url)
      .option("customSchema", customSchema)
  }
}
