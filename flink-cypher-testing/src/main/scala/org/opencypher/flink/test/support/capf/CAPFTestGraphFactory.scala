package org.opencypher.flink.test.support.capf

import org.opencypher.flink.{CAPFGraph, CAPFSession}
import org.opencypher.flink.CAPFConverters._
import org.opencypher.okapi.testing.propertygraph.{CreateGraphFactory, CypherTestGraphFactory}

trait CAPFTestGraphFactory extends CypherTestGraphFactory[CAPFSession] {
  def initGraph(createQuery: String)(implicit capf: CAPFSession): CAPFGraph = {
    apply(CreateGraphFactory(createQuery)).asCapf
  }
}
