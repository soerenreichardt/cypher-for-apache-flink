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

import org.apache.flink.table.api.Table
import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink._
import org.opencypher.flink.api.io.FlinkCypherTable.FlinkTable
import org.opencypher.flink.impl.{CAPFGraph, CAPFRecords, CAPFSession}
import org.opencypher.flink.impl.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.relational.api.physical.PhysicalOperator
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.trees.AbstractTreeNode

private[flink] abstract class CAPFPhysicalOperator
  extends AbstractTreeNode[CAPFPhysicalOperator]
  with PhysicalOperator[FlinkTable, CAPFRecords, CAPFGraph, CAPFRuntimeContext] {

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult

  protected def resolve(qualifiedGraphName: QualifiedGraphName)(implicit context: CAPFRuntimeContext): CAPFGraph = {
    context.resolve(qualifiedGraphName).map(_.asCapf).getOrElse(throw IllegalArgumentException(s"a graph at $qualifiedGraphName"))
  }

  protected def resolveTags(qgn: QualifiedGraphName)(implicit context: CAPFRuntimeContext): Set[Int] = context.patternGraphTags.getOrElse(qgn, resolve(qgn).tags)

  override def args: Iterator[Any] = super.args.flatMap {
    case RecordHeader | Some(RecordHeader)  => None
    case other                              => Some(other)
  }
}

