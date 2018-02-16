package org.opencypher.caps.flink

import org.opencypher.caps.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.caps.impl.exception.UnsupportedOperationException

object CAPFConverters {

  implicit class RichPropertyGraph(graph: PropertyGraph) {
    def asCapf: CAPFGraph = graph match {
      case capf: CAPFGraph  => capf
      case _                => throw UnsupportedOperationException(s"can only handle CAPS graphs, got $graph")
    }
  }

  implicit class RichSession(session: CypherSession) {
    def asCapf: CAPFSession = session match {
      case capf: CAPFSession  => capf
      case _                  => throw UnsupportedOperationException(s"can only handle CAPF sessions, got $session")
    }
  }

}
