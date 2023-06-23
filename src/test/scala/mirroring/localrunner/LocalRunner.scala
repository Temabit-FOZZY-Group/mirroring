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
package mirroring.localrunner

import wvlet.airframe.config.Config

object LocalRunner {

  def main(args: Array[String]): Unit = {

//    configPaths = Seq("/")
    val config: Config = Config().registerFromYaml[TestConfig]("test-config.yaml")

    val testConfig = config.of[TestConfig]

    println(testConfig)
//
//    val config: Config = ConfigBuilder.build(ConfigBuilder.parse(args))
//    val mirroringManager = new MirroringManager() with SparkContextTestTrait
//    mirroringManager.startDataMirroring(config)
  }
}
