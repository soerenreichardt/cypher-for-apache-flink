package org.opencypher.flink

import java.util.UUID

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.opencypher.flink.schema.{CAPFEntityTable, CAPFNodeTable}
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.api.value._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.io.SessionPropertyGraphDataSource
import org.opencypher.okapi.impl.util.Measurement.time
import org.opencypher.okapi.ir.api.IRExternalGraph
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintFlatPlan
import org.opencypher.okapi.relational.impl.flat.{FlatPlanner, FlatPlannerContext}

trait CAPFSession extends CypherSession {

  def env = ExecutionEnvironment.getExecutionEnvironment
  def tableEnv = TableEnvironment.getTableEnvironment(env)

  def readFrom(entityTables: CAPFEntityTable*): Unit = entityTables.head match {
    case h: CAPFNodeTable => readFrom(h, entityTables.tail: _*)
    case _ => throw IllegalArgumentException("first argument of type NodeTable", "RelationshipTable")
  }

  def readFrom(nodeTable: CAPFNodeTable, entityTables: CAPFEntityTable*): CAPFGraph = {
    CAPFGraph.create(nodeTable, entityTables: _*)(this)
  }
}

object CAPFSession {

//  def create: CAPFSession = new CAPFSessionImpl(CAPFGraphSourceHandler(CAPFSessionPropertyGraphDataSourceFactory(), Set.empty))

  def create(sessionNamespace: Namespace = SessionPropertyGraphDataSource.Namespace): CAPFSession =
    new CAPFSessionImpl(sessionNamespace)

}

sealed class CAPFSessionImpl(val sessionNamespace: Namespace) extends CAPFSession with Serializable {

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

  override def cypherOnGraph(graph: PropertyGraph, query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): CypherResult =
  {
    val ambientGraph = mountAmbientGraph(graph)

    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query)(CypherParser.defaultContext))

    val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = parameters ++ extractedParameters

    val ir = time("IR translation")(IRBuilder(stmt)(IRBuilderContext.initial(query, allParameters, semState, ambientGraph, dataSource)))

    val logicalPlannerContext = LogicalPlannerContext(graph.schema, Set.empty, ir.model.graphs.mapValues(_.namespace).andThen(dataSource), ambientGraph)
    val logicalPlan = time("Logical planning")(logicalPlanner(ir)(logicalPlannerContext))
    val optimizedLogicalPlan = time("Logical optimization")(logicalOptimizer(logicalPlan)(logicalPlannerContext))
    if (PrintLogicalPlan.isSet) {
      println(logicalPlan.pretty)
      println(optimizedLogicalPlan.pretty)
    }

    val flatPlan = time("Flat planning")(flatPlanner(optimizedLogicalPlan)(FlatPlannerContext(parameters)))
    if (PrintFlatPlan.isSet) println(flatPlan.pretty)

    ???
//    TODO: physical planning
  }

  private def mountAmbientGraph(ambient: PropertyGraph): IRExternalGraph = {
    val graphName = GraphName(UUID.randomUUID().toString)
    val qualifiedGraphName = store(graphName, ambient)
    IRExternalGraph(graphName.value, ambient.schema, qualifiedGraphName)
  }
}
