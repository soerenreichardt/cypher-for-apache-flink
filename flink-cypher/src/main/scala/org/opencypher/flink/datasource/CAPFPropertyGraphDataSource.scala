package org.opencypher.flink.datasource

import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema

trait CAPFPropertyGraphDataSource extends PropertyGraphDataSource {
  /**
    * Returns `true` if the data source stores a graph under the given [[org.opencypher.okapi.api.graph.GraphName]].
    *
    * @param name name of the graph within the data source
    * @return `true`, iff the graph is stored within the data source
    */
  override def hasGraph(name: GraphName): Boolean = ???

  /**
    * Returns the [[org.opencypher.okapi.api.graph.PropertyGraph]] that is stored under the given name.
    *
    * @param name name of the graph within the data source
    * @return property graph
    */
  override def graph(name: GraphName): PropertyGraph = ???

  /**
    * Returns the [[org.opencypher.okapi.api.schema.Schema]] of the graph that is stored under the given name.
    *
    * This method gives implementers the ability to efficiently retrieve a graph schema from the data source directly.
    * For reasons of performance, it is highly recommended to make a schema available through this call. If an efficient
    * retrieval is not possible, the call is typically forwarded to the graph using the [[org.opencypher.okapi.api.graph.PropertyGraph#schema]] call, which may require materialising the full graph.
    *
    * @param name name of the graph within the data source
    * @return graph schema
    */
  override def schema(name: GraphName): Option[Schema] = ???

  /**
    * Stores the given [[org.opencypher.okapi.api.graph.PropertyGraph]] under the given [[org.opencypher.okapi.api.graph.GraphName]] within the data source.
    *
    * @param name  name under which the graph shall be stored
    * @param graph property graph
    */
  override def store(name: GraphName, graph: PropertyGraph): Unit = ???

  /**
    * Deletes the [[org.opencypher.okapi.api.graph.PropertyGraph]] within the data source that is stored under the given [[org.opencypher.okapi.api.graph.GraphName]].
    *
    * @param name name under which the graph is stored
    */
  override def delete(name: GraphName): Unit = ???

  /**
    * Returns the [[org.opencypher.okapi.api.graph.GraphName]]s of all [[org.opencypher.okapi.api.graph.PropertyGraph]]s stored within the data source.
    *
    * @return names of stored graphs
    */
  override def graphNames: Set[GraphName] = ???
}
