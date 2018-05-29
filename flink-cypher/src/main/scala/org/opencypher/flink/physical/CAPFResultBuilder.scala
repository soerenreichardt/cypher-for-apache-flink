package org.opencypher.flink.physical

import org.opencypher.flink.physical.operators.CAPFPhysicalOperator
import org.opencypher.flink.{CAPFGraph, CAPFRecords, CAPFResult}
import org.opencypher.okapi.api.graph.CypherQueryPlans
import org.opencypher.okapi.logical.impl.LogicalOperator
import org.opencypher.okapi.relational.impl.flat.FlatOperator
import org.opencypher.okapi.trees.TreeNode

object CAPFResultBuilder {

  def from(logical: LogicalOperator, flat: FlatOperator, physical: CAPFPhysicalOperator)
          (implicit context: CAPFRuntimeContext): CAPFResult = {

    new CAPFResult {
      lazy val result: CAPFPhysicalResult = physical.execute

      override def records: Option[CAPFRecords] = Some(result.records)

      override def graph: Option[CAPFGraph] = Some(result.workingGraph)

      override def plans = CAPFQueryPlans(Some(logical), Some(flat), Some(physical))
    }
  }
}

case class CAPFQueryPlans(
  logicalPlan: Option[TreeNode[LogicalOperator]],
  flatPlan: Option[TreeNode[FlatOperator]],
  physicalPlan: Option[TreeNode[CAPFPhysicalOperator]]) extends CypherQueryPlans {

  override def logical: String = logicalPlan.map(_.pretty).getOrElse("")

  override def physical: String = physicalPlan.map(_.pretty).getOrElse("")

}

object CAPFQueryPlans {
  def empty: CAPFQueryPlans = CAPFQueryPlans(None, None, None)
}
