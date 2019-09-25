/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.ir.impl.parse.functions

import org.opencypher.v9_0.expressions.functions.Function
import org.opencypher.v9_0.util.symbols._

case object FunctionExtensions {

  private val mappings: Map[String, Function] = Map(
    Timestamp.name -> Timestamp,
    LocalDateTime.name -> LocalDateTime,
    Date.name -> Date,
    Duration.name -> Duration)
    .map(p => p._1.toLowerCase -> p._2)

  def get(name: String): Option[Function] =
    mappings.get(name.toLowerCase())

  def getOrElse(name: String, f: Function): Function =
    mappings.getOrElse(name.toLowerCase(), f)
}

case object Timestamp extends Function {
  override val name = "timestamp"
}

case object LocalDateTime extends Function {
  override val name = "localdatetime"
}

case object Date extends Function {
  override val name = "date"
}

case object Duration extends Function {
  override val name = "duration"
}

object CTIdentity extends CypherType {
  override def parentType: CypherType = CTAny
  override def toNeoTypeString: String = "IDENTITY"
}
