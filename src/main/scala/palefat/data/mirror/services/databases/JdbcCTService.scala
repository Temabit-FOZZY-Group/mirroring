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

import java.sql.DriverManager
import org.apache.spark.sql.DataFrame
import palefat.data.mirror.builders._
import wvlet.log.LogSupport

class JdbcCTService(jdbcContext: JdbcContext) extends BaseJdbcService(jdbcContext) with LogSupport {

  def loadData(): DataFrame = {
    val connection = DriverManager.getConnection(url)
    try {
      val params: Array[String] = JdbcBuilder.buildCTChangesQueryParams(
        jdbcContext.CTChangesQueryParams,
        jdbcContext.schema,
        jdbcContext.table,
        jdbcContext.changeTrackingLastVersion.toString(),
        jdbcContext.ctCurrentVersion.toString(),
      )
      val jdbcDF: DataFrame = JdbcBuilder.buildDataFrameFromResultSet(
        JdbcBuilder.buildJDBCResultSet(
          connection,
          jdbcContext.CTChangesQuery,
          params
        )
      )
      // spark.createDataFrame is lazy so action on jdbcDF is needed while ResultSet is open
      logger.info(s"Number of incoming rows: ${jdbcDF.count}")
      jdbcDF
    } catch {
      case e: Exception => throw e
    } finally {
      connection.close()
    }
  }
}
