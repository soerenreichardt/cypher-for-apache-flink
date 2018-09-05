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
package org.opencypher.flink.impl.physical

import org.opencypher.flink.impl.physical.operators._
import org.opencypher.okapi.ir.api.block.Asc
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.trees.TopDown

case class PhysicalOptimizerContext()

class PhysicalOptimizer extends DirectCompilationStage[CAPFPhysicalOperator, CAPFPhysicalOperator, PhysicalOptimizerContext] {

  override def process(input: CAPFPhysicalOperator)(implicit context: PhysicalOptimizerContext): CAPFPhysicalOperator = {
    InsertCachingOperators(input)
    PrecedLimitAndSkipWithOrderby(input)
  }

  object PrecedLimitAndSkipWithOrderby extends (CAPFPhysicalOperator => CAPFPhysicalOperator) {
    def apply(input: CAPFPhysicalOperator): CAPFPhysicalOperator = {
      TopDown[CAPFPhysicalOperator] {
        case skipOrLimit if skipOrLimit.isInstanceOf[Skip] || skipOrLimit.isInstanceOf[Limit] =>
          val withPreceededChildren: Array[CAPFPhysicalOperator] = skipOrLimit.children.map {
            case orderby if orderby.isInstanceOf[OrderBy] => orderby
            case other => OrderBy(other, Seq(Asc(other.header.idExpressions().head))).withNewChildren(Array(other))
          }
          skipOrLimit.withNewChildren(withPreceededChildren)
        case other => other
      }
    }.rewrite(input)
  }

  object InsertCachingOperators extends (CAPFPhysicalOperator => CAPFPhysicalOperator) {
    def apply(input: CAPFPhysicalOperator): CAPFPhysicalOperator = {
      val replacements = calculateReplacementMap(input).filterKeys {
        case _: Start => false
        case _        => true
      }

      val nodesToReplace = replacements.keySet

      TopDown[CAPFPhysicalOperator] {
        case cache: Cache => cache
        case parent if (parent.childrenAsSet intersect nodesToReplace).nonEmpty =>
          val newChildren = parent.children.map(c => replacements.getOrElse(c, c))
          parent.withNewChildren(newChildren)
      }.rewrite(input)
    }

    private def calculateReplacementMap(input: CAPFPhysicalOperator): Map[CAPFPhysicalOperator, CAPFPhysicalOperator] = {
      val opCounts = identifyDuplicates(input)
      val opsByHeight = opCounts.keys.toSeq.sortWith((a, b) => a.height > b.height)
      val (opsToCache, _) = opsByHeight.foldLeft(Set.empty[CAPFPhysicalOperator] -> opCounts) { (agg, currentOp) =>
        agg match {
          case (currentOpsToCache: Set[CAPFPhysicalOperator], currentCounts: Map[CAPFPhysicalOperator, Int]) =>
            val currentOpCount = currentCounts(currentOp)
            if (currentOpCount > 1) {
              val updatedOps = currentOpsToCache + currentOp
              val updatedCounts = currentCounts.map {
                case (op, count) => op -> (if (currentOp.containsTree(op)) count - 1 else count)
              }
              updatedOps -> updatedCounts
            } else {
              currentOpsToCache -> currentCounts
            }
        }
      }

      opsToCache.map(op => op -> Cache(op)).toMap
    }

    private def identifyDuplicates(input: CAPFPhysicalOperator): Map[CAPFPhysicalOperator, Int] = {
      input
        .foldLeft(Map.empty[CAPFPhysicalOperator, Int].withDefaultValue(0)) {
          case (agg, op) => agg.updated(op, agg(op) + 1)
        }
        .filter(_._2 > 1)
    }
  }
}
