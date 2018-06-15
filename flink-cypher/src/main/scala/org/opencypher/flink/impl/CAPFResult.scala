package org.opencypher.flink

import org.opencypher.flink.impl.physical.CAPFQueryPlans
import org.opencypher.okapi.api.graph.{CypherQueryPlans, CypherResult, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.impl.util.PrintOptions

import scala.reflect.runtime.universe.TypeTag

trait CAPFResult extends CypherResult {

  override def graph: Option[CAPFGraph]

  override def records: Option[CAPFRecords]

  override def getRecords: CAPFRecords = records.get

  override def getGraph: CAPFGraph = graph.get

  override def plans: CAPFQueryPlans

  override def show(implicit options: PrintOptions): Unit =
    records match {
      case Some(r) => r.show
      case None => options.stream.print("No results")
    }

  override def toString = this.getClass.getSimpleName

}

object CAPFResult {
  def empty(queryPlans: CAPFQueryPlans = CAPFQueryPlans.empty): CAPFResult = new CAPFResult {

    override def records: Option[CAPFRecords] = None

    override def graph: Option[CAPFGraph] = None

    override def plans: CAPFQueryPlans = queryPlans

  }
}
