/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.flink.impl.physical.operators

import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{If, UnresolvedFieldReference}
import org.opencypher.flink.api.Tags
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink.impl.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.flink.impl.{CAPFGraph, CAPFRecords}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.logical.impl.LogicalPatternGraph
import org.opencypher.okapi.relational.impl.physical.{CrossJoin, JoinType}
import org.opencypher.okapi.relational.impl.table.RecordHeader

private[flink] abstract class BinaryPhysicalOperator extends CAPFPhysicalOperator {

  def lhs: CAPFPhysicalOperator

  def rhs: CAPFPhysicalOperator

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = executeBinary(lhs.execute, rhs.execute)

  def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult
}

final case class Join(
  lhs: CAPFPhysicalOperator,
  rhs: CAPFPhysicalOperator,
  joinExprs: Seq[(Expr, Expr)],
  header: RecordHeader,
  joinType: JoinType)
  extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {

    val joinedRecords = joinType match {
      case CrossJoin =>
        val crossedTable = left.records.table.cross(right.records.table)(left.records.capf)
        CAPFRecords(header, crossedTable)(left.records.capf)
      case other =>
        left.records.join(right.records, joinType, joinExprs: _*)
    }

    CAPFPhysicalResult(joinedRecords, left.workingGraph, left.workingGraphName)
  }

}

final case class TabularUnionAll(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator) extends BinaryPhysicalOperator with InheritedHeader {

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val leftData = left.records.table
    val rightData = right.records.table.select(leftData.columns.map(UnresolvedFieldReference): _*)

    val unionedData = leftData.union(rightData)
    val records = CAPFRecords(header, unionedData)(left.records.capf)

    CAPFPhysicalResult(records, left.workingGraph, left.workingGraphName)
  }
}

final case class CartesianProduct(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator, header: RecordHeader)
  extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val data = left.records.table
    val otherData = right.records.table
    val newData = data.cross(otherData)(left.records.capf)

    val records = CAPFRecords(header, newData)(left.records.capf)
    CAPFPhysicalResult(records, left.workingGraph, left.workingGraphName)
  }
}

final case class ConstructGraph(
  lhs: CAPFPhysicalOperator,
  rhs: CAPFPhysicalOperator,
  construct: LogicalPatternGraph)
  extends BinaryPhysicalOperator {

  override def toString: String = {
    val entities = construct.clones.keySet ++ construct.newEntities.map(_.v)
    s"ConstructGraph(on=[${construct.onGraphs.mkString(", ")}], entities=[${entities.mkString(", ")}])"
  }

  override def header: RecordHeader = RecordHeader.empty

  private def pickFreeTag(tagStrategy: Map[QualifiedGraphName, Map[Int, Int]]): Int = {
    val usedTags = tagStrategy.values.flatMap(_.values).toSet
    Tags.pickFreeTag(usedTags)
  }

  private def identityRetaggings(g: CAPFGraph): (CAPFGraph, Map[Int, Int]) = {
    g -> g.tags.zip(g.tags).toMap
  }

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    ???
//    implicit val session: CAPFSession = left.records.capf
//
//    val onGraph = right.workingGraph
//    val unionTagStrategy: Map[QualifiedGraphName, Map[Int, Int]] = right.tagStrategy
//
//    val LogicalPatternGraph(schema, clonedVarsToInputVars, newEntities, sets, _, name) = construct
//
//    val matchGraphs: Set[QualifiedGraphName] = clonedVarsToInputVars.values.map(_.cypherType.graph.get).toSet
//    val allGraphs = unionTagStrategy.keySet ++ matchGraphs
//    val tagsForGraph: Map[QualifiedGraphName, Set[Int]] = allGraphs.map(qgn => qgn -> resolveTags(qgn)).toMap
//
//    val constructTagStrategy = computeRetaggings(tagsForGraph, unionTagStrategy)
//
//    val aliasClones = clonedVarsToInputVars.filter { case (alias, original) => alias != original }
//    val baseTable = left.records.addAliases(aliasClones)
//
//    val retaggedBaseTable = clonedVarsToInputVars.foldLeft(baseTable) { case (table, clone) =>
//      table.retagVariable(clone._1, constructTagStrategy(clone._2.cypherType.graph.get)9)
//    }
//
//    val (newEntityTags, tableWithConstructedEntities) = {
//      if (newEntities.isEmpty) {
//        Set.empty[Int] -> retaggedBaseTable
//      } else {
//        val newEntityTag = pickFreeTag(constructTagStrategy)
//        val entityTable = createEntities(newEntities, retaggedBaseTable, newEntityTag)
//        val entityTableWithProperties = sets.foldLeft(entityTable) {
//          case (table, SetPropertyItem(key, v, expr)) =>
//            constructProperty(v, key, expr, table)
//        }
//        Set(newEntityTag) -> entityTableWithProperties
//      }
//    }
//
//    val allInputVars = baseTable.header.internalHeader.fields
//    val originalVarsToKeep = clonedVarsToInputVars.keySet -- aliasClones.keySet
//    val varsToRemoveFromTable = allInputVars -- originalVarsToKeep
//    val patternGraphTable = tableWithConstructedEntities.removeVars(varsToRemoveFromTable)
//
//    val tagsUsed = constructTagStrategy.foldLeft(newEntityTags) {
//      case (tags, (qgn, remapping)) =>
//        val remappedTags = tagsFroGraph(qgn).map(remapping)
//        tags ++ remappedTags
//    }
//
//    val patternGraph = CAPFGraph.create(patternGraphTable, schema.asCapf, tagsUsed)
//
  }

}
