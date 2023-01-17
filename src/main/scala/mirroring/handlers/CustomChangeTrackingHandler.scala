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

package mirroring.handlers

import mirroring.Config
import mirroring.services.databases.JdbcCTService
import wvlet.log.LogSupport

class CustomChangeTrackingHandler(config: Config)
    extends ChangeTrackingHandler(config)
    with LogSupport {

  private lazy val jdbcContext                  = config.getJdbcContext
  private lazy val jdbcCTService: JdbcCTService = new JdbcCTService(jdbcContext)

  override lazy val ctCurrentVersion: BigInt = {
    logger.info(s"Querying current change tracking version from the source...")
    logger.info("Change Tracking: use custom CTCurrentVersionQuery")
    val version: BigInt =
      jdbcCTService.getChangeTrackingVersion(
        config.CTCurrentVersionQuery,
        config.CTCurrentVersionParams
      )

    logger.info(s"Current CT version for the MSSQL table: $version")
    version
  }

  override lazy val ctMinValidVersion: BigInt = {
    logger.info(
      s"Querying minimum valid change tracking version from the source..."
    )
    logger.info("Change Tracking: use custom CTMinValidVersionQuery")
    val version: BigInt =
      jdbcCTService.getChangeTrackingVersion(
        config.CTMinValidVersionQuery,
        config.CTMinValidVersionParams
      )

    logger.info(s"Min valid version for the MSSQL table: $version")
    version
  }

  override def query: String = ""
}
