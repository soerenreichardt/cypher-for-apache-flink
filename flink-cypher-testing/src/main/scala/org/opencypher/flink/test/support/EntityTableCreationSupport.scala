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
package org.opencypher.flink.test.support

import org.apache.flink.table.api.Table
import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.api.io.CAPFElementTable
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.io.conversion.ElementMapping
import org.opencypher.okapi.impl.util.StringEncodingUtilities._

trait EntityTableCreationSupport {

  def constructEntityTable(pattern: Pattern, table: Table)(implicit session: CAPFSession): CAPFElementTable = {
    val mapping = pattern.elements.foldLeft(ElementMapping.empty(pattern)) {
      case (acc, entity) =>

        val entityColumns = table.getSchema.getFieldNames.filter(_.startsWith(s"${entity.name}_"))

        val idMapping: Map[IdKey, String] = entityColumns.collect {
          case id if id.endsWith("_id") => SourceIdKey -> id
          case src if src.endsWith("_source") => SourceStartNodeKey -> src
          case tgt if tgt.endsWith("_target") => SourceEndNodeKey -> tgt
        }.toMap

        val propertyMapping: Map[String, String] = entityColumns.collect {
          case prop if prop.endsWith("_property") =>
            val encodedKey = prop.replaceFirst(s"${entity.name}_", "").replaceFirst("_property", "")
            encodedKey.decodeSpecialCharacters -> prop
        }.toMap

        acc.copy(
          properties = acc.properties.updated(entity, propertyMapping),
          idKeys = acc.idKeys.updated(entity, idMapping)
        )
    }

    CAPFElementTable.create(mapping, table)
  }

}
