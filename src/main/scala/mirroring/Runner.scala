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

package mirroring

import mirroring.builders.ConfigBuilder
import mirroring.config.{Config, FlowLogger}
import mirroring.services.{MirroringManager, SparkContextTrait}
import wvlet.log.LogSupport

object Runner extends LogSupport {

  def main(args: Array[String]): Unit = {
    // preliminary FlowLogger initialization in order to log config building
    FlowLogger.init()
    val config: Config = initConfig(args)

    val mirroringManager = new MirroringManager() with SparkContextTrait
    mirroringManager.startDataMirroring(config)

  }

  private def initConfig(args: Array[String]): Config = {
    val config: Config = ConfigBuilder.build(ConfigBuilder.parse(args))
    logger.debug(s"Parameters parsed: ${config.toString}")
    config
  }

}
