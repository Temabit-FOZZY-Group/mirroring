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

import mirroring.services.SparkService.spark
import wvlet.airframe.codec.MessageCodec
import wvlet.log.LogFormatter.appendStackTrace
import wvlet.log.{LogFormatter, LogLevel, LogRecord, Logger}

case class Record(
    level: String,
    sparkApplicationId: String,
    sparkApplicationAttempt: String,
    mirrorTaskName: String,
    loggerName: String,
    message: String
)

object FlowLogger {

  def init(schema: String, tab: String, logLvl: String): Unit = {
    Logger.init
    object CustomLogFormatter extends LogFormatter {
      override def formatLog(logRecord: LogRecord): String = {
        val record = Record(
          level = logRecord.level.toString,
          sparkApplicationId = if (spark != null) spark.sparkContext.applicationId else "null",
          sparkApplicationAttempt = spark.sparkContext.applicationAttemptId.getOrElse("1"),
          mirrorTaskName = s"mirroring_${schema}__$tab",
          loggerName = logRecord.getLoggerName,
          message = logRecord.getMessage
        )
        val codec       = MessageCodec.of[Record]
        val jsonMessage = codec.toJson(record)
        appendStackTrace(jsonMessage, logRecord)
      }
    }
    Logger.setDefaultLogLevel(LogLevel.apply(logLvl))
    Logger.setDefaultFormatter(CustomLogFormatter)
  }

}
