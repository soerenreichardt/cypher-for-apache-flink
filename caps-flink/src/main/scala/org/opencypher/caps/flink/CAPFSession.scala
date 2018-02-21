package org.opencypher.caps.flink

import java.net.URI
import java.util.UUID

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.opencypher.caps.api.graph.{CypherResult, CypherSession, PropertyGraph}
import org.opencypher.caps.api.io.{PersistMode, PropertyGraphDataSource}
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.flink.CAPFConverters._
import org.opencypher.caps.flink.datasource.{CAPFGraphSourceHandler, CAPFPropertyGraphDataSource, CAPFSessionPropertyGraphDataSourceFactory}
import org.opencypher.caps.flink.schema.{CAPFEntityTable, CAPFNodeTable}
import org.opencypher.caps.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.caps.impl.flat.FlatPlanner
import org.opencypher.caps.ir.api.IRExternalGraph
import org.opencypher.caps.ir.impl.parse.CypherParser
import org.opencypher.caps.logical.impl.{LogicalOperatorProducer, LogicalOptimizer, LogicalPlanner}

trait CAPFSession extends CypherSession {

  def env = ExecutionEnvironment.getExecutionEnvironment
  def tableEnv = TableEnvironment.getTableEnvironment(env)

  def readFrom(entityTables: CAPFEntityTable*): Unit = entityTables.head match {
    case h: CAPFNodeTable => readFrom(h, entityTables.tail: _*)
    case _ => throw IllegalArgumentException("first argument of type NodeTable", "RelationshipTable")
  }

  def readFrom(nodeTable: CAPFNodeTable, entityTables: CAPFEntityTable*): Unit = {
    CAPFGraph.create(nodeTable, entityTables: _*)(this)
  }
}

object CAPFSession {

  def create: CAPFSession = new CAPFSessionImpl(CAPFGraphSourceHandler(CAPFSessionPropertyGraphDataSourceFactory(), Set.empty))

}

sealed class CAPFSessionImpl(private val graphSourceHandler: CAPFGraphSourceHandler) extends CAPFSession with Serializable {

  self =>

  private implicit def capfSession = this
  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = LogicalOptimizer
  private val flatPlanner = new FlatPlanner
  //  private val capfPlanner = new CAPFPlanner()
  private val parser = CypherParser

  def sourceAt(uri: URI): PropertyGraphDataSource =
    graphSourceHandler.sourceAt(uri)(this)

  def optGraphAt(uri: URI): Option[CAPFGraph] =
    graphSourceHandler.optSourceAt(uri)(this).map(_.graph) match {
      case Some(graph) => Some(graph.asCapf)
      case None => None
    }

  /**
    * Executes a Cypher query in this session on the current ambient graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query
    */
  override def cypher(query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): CypherResult = ???
//    cypherOnGraph(CAPFGraph.empty(this), query, parameters, drivingTable)

  override def cypherOnGraph(graph: PropertyGraph, query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): CypherResult = ???
//  {
//    val ambientGraph = getAmbientGraph(graph)
//
//    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query)(CypherParser.defaultContext))
//
//    val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
//    val allParameters = parameters ++ extractedParameters
//
//    val ir = time("IR translation")(IRBuilder(stmt)(IRBuilderContext.initial(query, allParameters, semState, ambientGraph, sourceAt)))
//
//    val logicalPlannerContext = LogicalPlannerContext(graph.schema, Set.empty, ir.model.graphs.andThen(sourceAt), ambientGraph)
//    val logicalPlan = time("Logical planning")(logicalPlanner(ir)(logicalPlannerContext))
//    val optimizedLogicalPlan = time("Logical optimization")(logicalOptimizer(logicalPlan)(logicalPlannerContext))
//    if (PrintLogicalPlan.isSet) {
//      println(logicalPlan.pretty)
//      println(optimizedLogicalPlan.pretty)
//    }
//
//    val flatPlan = time("Flat planning")(flatPlanner(optimizedLogicalPlan)(FlatPlannerContext(parameters)))
//    if (PrintFlatPlan.isSet) println(flatPlan.pretty)
//
////    TODO: physical planning
//  }

  /**
    * Reads a graph from the argument URI.
    *
    * @param uri URI locating a graph
    * @return graph located at the URI
    */
  override def readFrom(uri: URI): PropertyGraph =
    graphSourceHandler.sourceAt(uri)(this).graph

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

  private def getAmbientGraph(ambient: PropertyGraph): IRExternalGraph = {
    val name = UUID.randomUUID().toString
    val uri = URI.create(s"session:///graphs/ambient/$name")

    val graphSource = new CAPFPropertyGraphDataSource {

      override val session: CypherSession = ???

      override def sourceForGraphAt(uri: URI): Boolean = uri == canonicalURI

      override def canonicalURI: URI = uri

      override def create: PropertyGraph = throw UnsupportedOperationException("Creation of an ambient graph")

      override def graph: PropertyGraph = ambient

      override def store(graph: PropertyGraph, mode: PersistMode): PropertyGraph =
        throw UnsupportedOperationException("Persisting an ambient graph")

      override def delete(): Unit = throw UnsupportedOperationException("Deletion of an ambient graph")
    }

    graphSourceHandler.mountSourceAt(graphSource, uri)(self)

    IRExternalGraph(name, ambient.schema, uri)
  }
}
