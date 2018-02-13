package org.opencypher.caps.flink

import org.opencypher.caps.api.exception.UnsupportedOperationException
import org.opencypher.caps.api.graph.PropertyGraph

object CAPFConverters {

  implicit class RichPropertyGraph(graph: PropertyGraph) {
    def asCapf: CAPFGraph = graph match {
      case capf: CAPFGraph  => capf
      case _                => throw UnsupportedOperationException(s"can only handle CAPS graphs, got $graph")
    }
  }

}
