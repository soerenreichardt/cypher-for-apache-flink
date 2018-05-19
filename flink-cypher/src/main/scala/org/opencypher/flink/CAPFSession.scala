package org.opencypher.flink

import java.util.UUID

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.opencypher.flink.schema.{CAPFEntityTable, CAPFNodeTable}
import org.opencypher.flink.CAPFConverters._
import org.opencypher.flink.physical.{CAPFPhysicalOperatorProducer, CAPFPhysicalPlannerContext, CAPFResultBuilder, CAPFRuntimeContext}
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
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.{PrintFlatPlan, PrintPhysicalPlan}
import org.opencypher.okapi.relational.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.okapi.relational.impl.physical.PhysicalPlanner

trait CAPFSession extends CypherSession {

  val env: ExecutionEnvironment
  val tableEnv: BatchTableEnvironment

  def sql(query: String): CypherRecords

  def readFrom(entityTables: CAPFEntityTable*): PropertyGraph = entityTables.head match {
    case h: CAPFNodeTable => readFrom(h, entityTables.tail: _*)
    case _ => throw IllegalArgumentException("first argument of type NodeTable", "RelationshipTable")
  }

  def readFrom(nodeTable: CAPFNodeTable, entityTables: CAPFEntityTable*): CAPFGraph = {
    CAPFGraph.create(nodeTable, entityTables: _*)(this)
  }
}

object CAPFSession {

  val env = ExecutionEnvironment.getExecutionEnvironment
  val tableEnv = TableEnvironment.getTableEnvironment(env)

  def create(sessionNamespace: Namespace = SessionPropertyGraphDataSource.Namespace): CAPFSession =
    new CAPFSessionImpl(env, tableEnv, sessionNamespace)

  def local(): CAPFSession = {
    env.getConfig.disableSysoutLogging()
    create()
  }

}

sealed class CAPFSessionImpl(
  override val env: ExecutionEnvironment,
  override val tableEnv: BatchTableEnvironment,
  val sessionNamespace: Namespace) extends CAPFSession with Serializable {

  self =>

  private implicit def capfSession = this
  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = LogicalOptimizer
  private val flatPlanner = new FlatPlanner
  private val physicalPlanner = new PhysicalPlanner(new CAPFPhysicalOperatorProducer()(self))
  private val physicalOptimizer = new PhysicalOptimizer()
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

    val irBuilderContext = IRBuilderContext.initial(query, allParameters, semState, ambientGraph, dataSource)
    val ir = time("IR translation")(IRBuilder(stmt)(irBuilderContext))

    val logicalPlannerContext = LogicalPlannerContext(graph.schema, Set.empty, ir.model.graphs.mapValues(_.namespace).andThen(dataSource), ambientGraph)
    val logicalPlan = time("Logical planning")(logicalPlanner(ir)(logicalPlannerContext))
    if (PrintLogicalPlan.isSet) {
      println("Logical plan:")
      println(logicalPlan.pretty)
    }

    val optimizedLogicalPlan = time("Logical optimization")(logicalOptimizer(logicalPlan)(logicalPlannerContext))
    if (PrintLogicalPlan.isSet) {
      println("Optimized logical plan:")
      println(optimizedLogicalPlan.pretty())
    }

    plan(CAPFRecords.unit(), allParameters, optimizedLogicalPlan)
  }

  private def graphAt(qualifiedGraphName: QualifiedGraphName): Option[CAPFGraph] =
    Some(dataSource(qualifiedGraphName.namespace).graph(qualifiedGraphName.graphName).asCapf)

  private def plan(
    records: CypherRecords,
    parameters: CypherMap,
    logicalPlan: LogicalOperator): CAPFResult = {
    val flatPlannerContext = FlatPlannerContext(parameters)
    val flatPlan = time("Flat planning")(flatPlanner(logicalPlan)(flatPlannerContext))
    if (PrintFlatPlan.isSet) {
      println("Flat plan:")
      println(flatPlan.pretty)
    }

    val physicalPlannerContext = CAPFPhysicalPlannerContext.from(this.graph, records.asCapf, parameters)(self)
    val physicalPlan = time("Physical planning")(physicalPlanner(flatPlan)(physicalPlannerContext))
    if (PrintPhysicalPlan.isSet) {
      println("Physical plan:")
      println(physicalPlan.pretty)
    }

    val optimizedPhysicalPlan = time("Physical optimization")(physicalOptimizer(physicalPlan)(PhysicalOptimizerContext()))
    if (PrintPhysicalPlan.isSet) {
      println("Optimized physical plan:")
      println(optimizedPhysicalPlan.pretty)
    }

    CAPFResultBuilder.from(logicalPlan, flatPlan, optimizedPhysicalPlan)(
      CAPFRuntimeContext(physicalPlannerContext.parameters, graphAt, collection.mutable.Map.empty)
    )
  }

  private def mountAmbientGraph(ambient: PropertyGraph): IRExternalGraph = {
    val graphName = GraphName(UUID.randomUUID().toString)
    val qualifiedGraphName = store(graphName, ambient)
    IRExternalGraph(graphName.value, ambient.schema, qualifiedGraphName)
  }

  override def sql(query: String): CAPFRecords =
    CAPFRecords.wrap(tableEnv.sqlQuery(query))
}
