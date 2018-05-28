package org.opencypher.flink.physical.operators

import org.opencypher.flink.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.flink.{CAPFRecords, CAPFSession}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.relational.impl.flat.Start
import org.opencypher.okapi.relational.impl.table.RecordHeader

private[flink] abstract class LeafPhysicalOperator extends CAPFPhysicalOperator {

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = executeLeaf()

  def executeLeaf()(implicit context: CAPFRuntimeContext): CAPFPhysicalResult
}

object Start {

  def apply(qgn: QualifiedGraphName, records: CAPFRecords)(implicit capf: CAPFSession): Start = {
    Start(qgn, Some(records))
  }
}

final case class Start(qgn: QualifiedGraphName, recordsOpt: Option[CAPFRecords])
  (implicit capf: CAPFSession) extends LeafPhysicalOperator {

  override val header = recordsOpt.map(_.header).getOrElse(RecordHeader.empty)

  override def executeLeaf()(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val records = recordsOpt.getOrElse(CAPFRecords.unit())
    CAPFPhysicalResult(records, resolve(qgn), qgn)
  }

  override def toString = {
    val graphArg = qgn.toString
    val recordsArg = recordsOpt.map(_.toString)
    val allArgs = List(recordsArg, graphArg).mkString(",  ")
    s"Start($allArgs)"
  }

}