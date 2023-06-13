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

package mirroring.config

import wvlet.airframe.codec.MessageCodec
import wvlet.log.LogFormatter.{SourceCodeLogFormatter, appendStackTrace}
import wvlet.log.{LogFormatter, LogLevel, LogRecord, Logger}

import java.text.SimpleDateFormat
import java.util.Calendar

object FlowLogger {

  private val datetimeFormatter = new SimpleDateFormat("yy/MM/dd hh:mm:ss")

  def init(logLevel: LogLevel = LogLevel.INFO): Unit = {
    Logger.init
    Logger("mirroring").setLogLevel(logLevel)
    Logger.setDefaultFormatter(SourceCodeLogFormatter)
  }

  def init(loggerConfig: LoggerConfig): Unit = {
    Logger.init
    Logger("mirroring").setLogLevel(LogLevel.apply(loggerConfig.logLevel))
    Logger.setDefaultFormatter(CustomLogFormatter(loggerConfig))
  }

  def setLogFormatter(loggerConfig: LoggerConfig): Unit = {
    Logger.setDefaultFormatter(CustomLogFormatter(loggerConfig))
  }

  private def getCurrentTimestamp: java.util.Date = {
    Calendar.getInstance().getTime
  }

  case class CustomLogFormatter(loggerConfig: LoggerConfig) extends LogFormatter {

    override def formatLog(logRecord: LogRecord): String = {
      val record = LoggerRecord(
        timestamp = datetimeFormatter.format(getCurrentTimestamp),
        level = logRecord.level.toString,
        sparkApplicationId = loggerConfig.applicationId,
        sparkApplicationAttempt = loggerConfig.applicationAttemptId,
        mirrorTaskName = s"mirroring_${loggerConfig.schema}__${loggerConfig.table}",
        loggerName = logRecord.getLoggerName,
        message = logRecord.getMessage
      )
      val codec       = MessageCodec.of[LoggerRecord]
      val jsonMessage = codec.toJson(record)
      appendStackTrace(jsonMessage, logRecord)
    }
  }

}
