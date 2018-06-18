package org.opencypher.flink.impl.io

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.impl.exception.GraphAlreadyExistsException

trait CAPFPropertyGraphDataSource extends PropertyGraphDataSource {

  protected def checkStorable(name: GraphName): Unit = {
    if (hasGraph(name))
      throw GraphAlreadyExistsException(s"A graph with name $name is already stored in this graph data source.")
  }
}