package org.opencypher.flink

import org.opencypher.flink.physical.operators.{CAPFPhysicalOperator, Cache, Start, StartFromUnit}
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.trees.TopDown

case class PhysicalOptimizerContext()

class PhysicalOptimizer extends DirectCompilationStage[CAPFPhysicalOperator, CAPFPhysicalOperator, PhysicalOptimizerContext] {

  override def process(input: CAPFPhysicalOperator)(implicit context: PhysicalOptimizerContext): CAPFPhysicalOperator = {
    InsertCachingOperators(input)
  }

  object InsertCachingOperators extends (CAPFPhysicalOperator => CAPFPhysicalOperator) {
    def apply(input: CAPFPhysicalOperator): CAPFPhysicalOperator = {
      val replacements = calculateReplacementMap(input).filterKeys {
        case _: Start | _: StartFromUnit  => false
        case _                            => true
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
