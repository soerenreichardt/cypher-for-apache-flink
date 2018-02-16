package org.opencypher.caps.flink

import java.net.URI

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.opencypher.caps.api.graph.{CypherResult, CypherSession, PropertyGraph}
import org.opencypher.caps.api.io.{PersistMode, PropertyGraphDataSource}
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.impl.flat.FlatPlanner
import org.opencypher.caps.ir.impl.parse.CypherParser
import org.opencypher.caps.logical.impl.{LogicalOperatorProducer, LogicalOptimizer, LogicalPlanner}

trait CAPFSession extends CypherSession {

  def env = ExecutionEnvironment.getExecutionEnvironment
  def tableEnv = TableEnvironment.getTableEnvironment(env)

  def readFrom(nodes: Table, rels: Table): Unit = {
    implicit val session: CAPFSession = this
    CAPFGraph.create(nodes, rels)
  }

}

object CAPFSession {

  def create: CAPFSession = new CAPFSessionImpl

}

sealed class CAPFSessionImpl extends CAPFSession with Serializable {

  self =>

  private implicit def capfSession = this
  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = LogicalOptimizer
  private val flatPlanner = new FlatPlanner
  //  private val capfPlanner = new CAPFPlanner()
  private val parser = CypherParser

  /**
    * Executes a Cypher query in this session on the current ambient graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query
    */
  override def cypher(query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): CypherResult =
    cypherOnGraph(CAPFGraph.empty(this), query, parameters, drivingTable)

  override def cypherOnGraph(graph: PropertyGraph, query: String, queryParameters: CypherMap, maybeDrivingTable: Option[CypherRecords]): CypherResult = ???

  /**
    * Reads a graph from the argument URI.
    *
    * @param uri URI locating a graph
    * @return graph located at the URI
    */
  override def readFrom(uri: URI): PropertyGraph = ???

  /**
    * Mounts the given graph source to session-local storage under the given path. The specified graph will be
    * accessible under the session-local URI scheme, e.g. {{{session://$path}}}.
    *
    * @param source graph source to register
    * @param path   path at which this graph can be accessed via {{{session://$path}}}
    */
  override def mount(source: PropertyGraphDataSource, path: String): Unit = ???

  /**
    * Writes the given graph to the location using the format specified by the URI.
    *
    * @param graph graph to write
    * @param uri   graph URI indicating location and format to write the graph to
    * @param mode  persist mode which determines what happens if the location is occupied
    */
  override def write(graph: PropertyGraph, uri: String, mode: PersistMode): Unit = ???

  /**
    * Mounts the given property graph to session-local storage under the given path. The specified graph will be
    * accessible under the session-local URI scheme, e.g. {{{session://$path}}}.
    *
    * @param graph property graph to register
    * @param path  path at which this graph can be accessed via {{{session://$path}}}
    */
  override def mount(graph: PropertyGraph, path: String): Unit = ???
}
