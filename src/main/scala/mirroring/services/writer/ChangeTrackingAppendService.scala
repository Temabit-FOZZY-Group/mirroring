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

package mirroring.services.writer

import mirroring.config.UserMetadata
import mirroring.services.SparkContextTrait
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import wvlet.airframe.codec.MessageCodec
import wvlet.log.Logger

class ChangeTrackingAppendService(
    context: WriterContext
) extends DeltaService(context)
    with Serializable {
  this: SparkContextTrait =>

  override val logger: Logger = Logger.of[ChangeTrackingAppendService]

  val userMetadataJSON: String = generateUserMetadataJSON(context.ctCurrentVersion)

  override def dfWriter(data: DataFrame): DataFrameWriter[Row] = {
    super.dfWriter(data).option("userMetadata", userMetadataJSON)
  }

  private def generateUserMetadataJSON(ctCurrentVersion: BigInt): String = {
    val userMetadata             = UserMetadata(ctCurrentVersion)
    val codec                    = MessageCodec.of[UserMetadata]
    val userMetadataJSON: String = codec.toJson(userMetadata)
    userMetadataJSON
  }

  override def getUserMetadataJSON: String = {
    userMetadataJSON
  }
}
