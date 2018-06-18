package org.opencypher.flink.test.support.capf

import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.flink.impl.{CAPFGraph, CAPFSession}
import org.opencypher.okapi.testing.propertygraph.{CreateGraphFactory, CypherTestGraphFactory}

trait CAPFTestGraphFactory extends CypherTestGraphFactory[CAPFSession] {
  def initGraph(createQuery: String)(implicit capf: CAPFSession): CAPFGraph = {
    apply(CreateGraphFactory(createQuery)).asCapf
  }
}
