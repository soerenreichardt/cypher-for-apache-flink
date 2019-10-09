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
package org.opencypher.morpheus.impl.convert

import org.apache.spark.sql.Row
import org.opencypher.morpheus.api.value.{MorpheusNode, MorpheusRelationship}
import org.opencypher.morpheus.impl.convert.SparkConversions._
import org.opencypher.okapi.api.types.{CTList, CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.api.value._
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api.expr.{Expr, ListSegment, Var}
import org.opencypher.okapi.relational.impl.table.RecordHeader

// TODO: argument cannot be a Map due to Scala issue https://issues.scala-lang.org/browse/SI-7005
final case class rowToCypherMap(exprToColumn: Seq[(Expr, String)]) extends (Row => CypherMap) {

  private val header = RecordHeader(exprToColumn.toMap)

  override def apply(row: Row): CypherMap = {
    val values = header.returnItems.map(r => r.name -> constructValue(row, r)).toSeq
    CypherMap(values: _*)
  }

  // TODO: Validate all column types. At the moment null values are cast to the expected type...
  private def constructValue(row: Row, v: Var): CypherValue = {
    v.cypherType.material match {
      case n if n.subTypeOf(CTNode.nullable) => collectNode(row, v)
      case r if r.subTypeOf(CTRelationship.nullable) => collectRel(row, v)
      case l if l.subTypeOf(CTList.nullable) && !header.exprToColumn.contains(v) => collectComplexList(row, v)
      case _ => constructFromExpression(row, v)
    }
  }

  private def constructFromExpression(row: Row, expr: Expr): CypherValue = {
    val raw = row.getAs[Any](header.column(expr))
    CypherValue(raw)
  }

  private def collectNode(row: Row, v: Var): CypherValue = {
    val idValue = row.getAs[Any](header.column(v))
    idValue match {
      case null => CypherNull
      case id: Long =>

        val labels = header
          .labelsFor(v)
          .map { l => l.label.name -> row.getAs[Boolean](header.column(l)) }
          .collect { case (name, true) => name }

        val properties = header
          .propertiesFor(v)
          .map { p => p.key.name -> constructFromExpression(row, p) }
          .collect { case (key, value) if !value.isNull => key -> value }
          .toMap

        MorpheusNode(id, labels, properties)
      case invalidID => throw UnsupportedOperationException(s"MorpheusNode ID has to be an Array[Byte] instead of ${invalidID.getClass}")
    }
  }

  private def collectRel(row: Row, v: Var): CypherValue = {
    val idValue = row.getAs[Any](header.column(v))
    idValue match {
      case null => CypherNull
      case id: Long =>
        val source = row.getAs[Long](header.column(header.startNodeFor(v)))
        val target = row.getAs[Long](header.column(header.endNodeFor(v)))

        val relType = header
          .typesFor(v)
          .map { l => l.relType.name -> row.getAs[Boolean](header.column(l)) }
          .collect { case (name, true) => name }
          .head

        val properties = header
          .propertiesFor(v)
          .map { p => p.key.name -> constructFromExpression(row, p) }
          .collect { case (key, value) if !value.isNull => key -> value }
          .toMap

        MorpheusRelationship(
          id.asInstanceOf[Long],
          source.asInstanceOf[Long],
          target.asInstanceOf[Long],
          relType,
          properties)
      case invalidID => throw UnsupportedOperationException(s"MorpheusRelationship ID has to be an Array[Byte] instead of ${invalidID.getClass}")
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
