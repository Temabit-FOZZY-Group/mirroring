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

import com.sun.org.apache.bcel.internal.classfile.JavaClass
import mirroring.builders.ConfigBuilder
import wvlet.airframe.config.Config
import mirroring.config.{FlowLogger, Config => AppConfig}
import mirroring.services.MirroringManager

object LocalRunner {

  // Set env variable for "MSSQL_USER" and "MSSQL_PASSWORD" before running
  def main(args: Array[String]): Unit = {
    FlowLogger.init()
    val config: Config = Config().registerFromYaml[TestConfig]("test-config.yaml")
    val testConfig = config.of[TestConfig]
    val applicationArguments = testConfig.getApplicationArguments

    println(applicationArguments.mkString(" "))

    val appConfig: AppConfig = ConfigBuilder.build(ConfigBuilder.parse(applicationArguments))
    println(appConfig.toString)

    val mirroringManager = new MirroringManager() with SparkContextTestTrait
    mirroringManager.startDataMirroring(appConfig)

  }


}
