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

object DatatypeMapping {
  val DatatypeMapping: scala.collection.mutable.Map[String, String] =
    scala.collection.mutable.Map[String, String](
      "date" -> "DATE"
    )

  def getDatatype(incomingDatatype: String): String = {
    DatatypeMapping(incomingDatatype.toLowerCase.trim)
  }

  def contains(incomingDatatype: String): Boolean = {
    DatatypeMapping.contains(incomingDatatype.toLowerCase.trim)
  }
}
