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

headerLicense := Some(
  HeaderLicense.Custom(
    """|Copyright (2021) The Delta Flow Project Authors.
     |
     |Licensed under the Apache License, Version 2.0 (the "License");
     |you may not use this file except in compliance with the License.
     |You may obtain a copy of the License at
     |
     |http://www.apache.org/licenses/LICENSE-2.0
     |
     |Unless required by applicable law or agreed to in writing, software
     |distributed under the License is distributed on an "AS IS" BASIS,
     |WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     |See the License for the specific language governing permissions and
     |limitations under the License.
     |""".stripMargin
  )
)
name := "DeltaFlow"
scalaVersion := "2.13.5"
val sparkVersion = "3.2.3"
version := s"1.0.4-spark${sparkVersion}-scala${scalaVersion.value}"
libraryDependencies ++= Seq(
  "org.apache.spark"   %% "spark-core"         % sparkVersion % "provided",
  "org.apache.spark"   %% "spark-sql"          % sparkVersion % "provided",
  "org.apache.spark"   %% "spark-hive"         % sparkVersion % "provided",
  "io.delta"           %% "delta-core"         % "2.0.1"      % "provided",
  "org.wvlet.airframe" %% "airframe-log"       % "22.7.3",
  "org.wvlet.airframe" %% "airframe-json"      % "22.8.0",
  "org.wvlet.airframe" %% "airframe-codec"     % "22.8.0",
  "org.scalatest"      %% "scalatest"          % "3.2.14"     % "test",
  "org.scalatest"      %% "scalatest-core"     % "3.2.13"     % "test",
  "org.scalatest"      %% "scalatest-funsuite" % "3.2.13"     % "test"
)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
