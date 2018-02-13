/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.caps.ir.impl.typer

import org.neo4j.cypher.internal.util.v3_4.{symbols => frontend}
import org.opencypher.caps.api.types._

case object toFrontendType extends (CypherType => frontend.CypherType) {
  override def apply(in: CypherType): frontend.CypherType = in.material match {
    case CTAny          => frontend.CTAny
    case CTNumber       => frontend.CTNumber
    case CTInteger      => frontend.CTInteger
    case CTFloat        => frontend.CTFloat
    case CTBoolean      => frontend.CTBoolean
    case CTString       => frontend.CTString
    case CTNode         => frontend.CTNode
    case CTRelationship => frontend.CTRelationship
    case CTPath         => frontend.CTPath
    case CTMap          => frontend.CTMap
    case CTList(inner)  => frontend.ListType(toFrontendType(inner))
    case x => throw new UnsupportedOperationException(s"Can not convert CAPS type $x to an openCypher frontend type")
  }
}
