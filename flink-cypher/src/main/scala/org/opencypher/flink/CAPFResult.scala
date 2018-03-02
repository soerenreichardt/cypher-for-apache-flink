package org.opencypher.flink

import org.opencypher.flink.physical.CAPFQueryPlans
import org.opencypher.okapi.api.graph.{CypherQueryPlans, CypherResult, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.impl.util.PrintOptions

import scala.reflect.runtime.universe.TypeTag

trait CAPFResult extends CypherResult {
  /**
    * Retrieves the graph if one is returned by the query.
    * If the query returns a table, `None` is returned.
    *
    * @return a graph if the query returned one, `None` otherwise
    */
  override def graph: Option[PropertyGraph]

  /**
    * The table of records if one was returned by the query.
    * Returns `None` if the query returned a graph.
    *
    * @return a table of records, `None` otherwise.
    */
  override def records: Option[CypherRecords]

  /**
    * API for printable plans. This is used for explaining the execution plan of a Cypher query.
    */
  override def plans: CAPFQueryPlans

  override def show(implicit options: PrintOptions): Unit =
    records match {
      case Some(r) => r.show
      case None => options.stream.print("No results")
    }

  override def toString = this.getClass.getSimpleName

}
