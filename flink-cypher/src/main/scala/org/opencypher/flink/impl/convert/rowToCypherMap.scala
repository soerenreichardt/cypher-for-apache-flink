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
package org.opencypher.flink.impl.convert

import org.apache.flink.types.Row
import org.opencypher.flink.api.value.{CAPFNode, CAPFRelationship}
import org.opencypher.okapi.api.types.{CTList, CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherList, CypherMap, CypherNull, CypherValue}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api.expr.{Expr, ListSegment, Var}
import org.opencypher.okapi.relational.impl.table.RecordHeader

final case class rowToCypherMap(exprToColumn: Seq[(Expr, String)], columnNameToIndex: Map[String, Int]) extends (Row => CypherMap) {

  private val header = RecordHeader(exprToColumn.toMap)

  override def apply(row: Row): CypherMap = {
    val values = header.returnItems.map(r => r.name -> constructValue(row, r)).toSeq
    CypherMap(values: _*)
  }

  private def constructValue(row: Row, v: Var): CypherValue = {
    v.cypherType.material match {
      case _: CTNode =>
        collectNode(row, v)

      case _: CTRelationship =>
        collectRel(row, v)

      case CTList(_) if !header.exprToColumn.contains(v) =>
        collectComplexList(row, v)

      case _ =>
        import scala.collection.JavaConverters._
        val raw = row.getField(columnNameToIndex(header.column(v))) match {
          case map: java.util.HashMap[_, _] => map.asScala.toMap
          case other => other
        }
        CypherValue(raw)
    }
  }

  private def collectNode(row: Row, v: Var): CypherValue = {
    val idValue = row.getField(columnNameToIndex(header.column(v))).asInstanceOf[Any]
    idValue match {
      case null => CypherNull
      case id: Long =>

        val labels = header
          .labelsFor(v)
          .map { l => l.label.name -> row.getField(columnNameToIndex(header.column(l))).asInstanceOf[Boolean] }
          .collect { case (name, true) => name }

        val properties = header
          .propertiesFor(v)
          .map { p => p.key.name -> CypherValue(row.getField(columnNameToIndex(header.column(p)))) }
          .collect { case (key, value) if !value.isNull => key -> value }
          .toMap

        CAPFNode(id, labels, properties)
      case invalidID => throw UnsupportedOperationException(s"CAPFNode ID has to be a Long instead of ${invalidID.getClass}")
    }
  }

  private def collectRel(row: Row, v: Var): CypherValue = {
    val idValue = row.getField(columnNameToIndex(header.column(v))).asInstanceOf[Any]
    idValue match {
      case null => CypherNull
      case id: Long =>
        val source = row.getField(columnNameToIndex(header.column(header.startNodeFor(v)))).asInstanceOf[Long]
        val target = row.getField(columnNameToIndex(header.column(header.startNodeFor(v)))).asInstanceOf[Long]

        val relType = header
          .typesFor(v)
          .map { l => l.relType.name -> row.getField(columnNameToIndex(header.column(l))).asInstanceOf[Boolean] }
          .collect { case (name, true) => name }
          .head

        val properties = header
          .propertiesFor(v)
          .map { p => p.key.name -> CypherValue(row.getField(columnNameToIndex(header.column(p))).asInstanceOf[Any]) }
          .collect { case (key, value) if !value.isNull => key -> value }
          .toMap

        CAPFRelationship(id, source, target, relType, properties)
      case invalidID => throw UnsupportedOperationException(s"CAPFRelationship ID has to be a Long instead of ${invalidID.getClass}")
    }
  }

  private def collectComplexList(row: Row, expr: Var): CypherList = {
    val elements = header.ownedBy(expr).collect {
      case p: ListSegment => p
    }.toSeq.sortBy(_.index)

    val values = elements
      .map(constructValue(row, _))
      .filter {
        case CypherNull => false
        case _ => true
      }

    CypherList(values)
  }
}