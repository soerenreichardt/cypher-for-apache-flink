package org.opencypher.flink.datasource

import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema

trait CAPFPropertyGraphDataSource extends PropertyGraphDataSource