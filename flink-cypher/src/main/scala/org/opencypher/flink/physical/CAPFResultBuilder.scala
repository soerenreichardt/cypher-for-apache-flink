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

      override def graph: Option[CAPFGraph] = result.graphs.values.headOption

      override def plans = CAPFQueryPlans(logical, flat, physical)
    }
  }
}

case class CAPFQueryPlans(
  logicalPlan: TreeNode[LogicalOperator],
  flatPlan: TreeNode[FlatOperator],
  physicalPlan: TreeNode[CAPFPhysicalOperator]) extends CypherQueryPlans {

  override def logical: String = logicalPlan.pretty

  override def physical: String = physicalPlan.pretty

}
