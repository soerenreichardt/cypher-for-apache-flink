package org.opencypher.flink

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.opencypher.flink.CAPFConverters._
import org.opencypher.flink.physical.{CAPFPhysicalOperatorProducer, CAPFPhysicalPlannerContext, CAPFResultBuilder, CAPFRuntimeContext}
import org.opencypher.flink.schema.{CAPFEntityTable, CAPFNodeTable}
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.api.value._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.graph.CypherCatalog
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.okapi.impl.util.Measurement.time
import org.opencypher.okapi.ir.api.configuration.IrConfiguration.PrintIr
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext, QGNGenerator, QueryCatalog}
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.{PrintFlatPlan, PrintOptimizedPhysicalPlan, PrintPhysicalPlan, PrintQueryExecutionStages}
import org.opencypher.okapi.relational.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.okapi.relational.impl.physical.PhysicalPlanner

trait CAPFSession extends CypherSession {

  override val catalog: CypherCatalog = new CypherCatalog

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

  private[opencypher] val emptyGraphQgn = QualifiedGraphName(catalog.sessionNamespace, GraphName("emptyGraph"))

  catalog.store(emptyGraphQgn, CAPFGraph.empty(this))
}

object CAPFSession {

  val env = ExecutionEnvironment.getExecutionEnvironment
  val tableEnv = TableEnvironment.getTableEnvironment(env)

  def create(): CAPFSession =
    new CAPFSessionImpl(env, tableEnv)

  def local(): CAPFSession = {
    env.getConfig.disableSysoutLogging()
    create()
  }

}

sealed class CAPFSessionImpl(
  override val env: ExecutionEnvironment,
  override val tableEnv: BatchTableEnvironment)
  extends CAPFSession
    with Serializable
    with CAPFSessionOps {

  self =>

  private implicit def capfSession = this

  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = LogicalOptimizer
  private val flatPlanner = new FlatPlanner
  private val physicalPlanner = new PhysicalPlanner(new CAPFPhysicalOperatorProducer()(self))
  private val physicalOptimizer = new PhysicalOptimizer()
  private val parser = CypherParser

  private val maxSessionGraphId: AtomicLong = new AtomicLong(0)

  /**
    * Executes a Cypher query in this session on the current ambient graph.
    *
    * @param query      Cypher query to execute
    * @param parameters parameters used by the Cypher query
    * @return result of the query
    */
  override def cypher(query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): CypherResult =
    cypherOnGraph(CAPFGraph.empty(this), query, parameters, drivingTable)

  override def cypherOnGraph(graph: PropertyGraph, query: String, queryParameters: CypherMap, maybeDrivingTable: Option[CypherRecords]): CypherResult =
  {
    val ambientGraph = mountAmbientGraph(graph)

    val drivingTable = maybeDrivingTable.getOrElse(CAPFRecords.unit())
    val inputFields = drivingTable.asCapf.header.internalHeader.fields

    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query,  inputFields)(CypherParser.defaultContext))

    val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = queryParameters ++ extractedParameters

    logStageProgress("IR translation ...", newLine = false)

    val irBuilderContext = IRBuilderContext.initial(query, allParameters, semState, ambientGraph, qgnGenerator, catalog.listSources, inputFields)
    val irOut = time("IR translation")(IRBuilder.process(stmt)(irBuilderContext))

    val ir = IRBuilder.extract(irOut)

    logStageProgress("Done!")

    if (PrintIr.isSet) {
      println("IR:")
      println(ir.pretty)
    }

    ir match {
      case cq: CypherQuery[Expr] =>
        planCypherQuery(graph, cq, allParameters, inputFields, drivingTable)

      case CreateGraphStatement(_, targetGraph, innerQueryIr) =>
        val innerResult = planCypherQuery(graph, innerQueryIr, allParameters, inputFields, drivingTable)
        val resultGraph = innerResult.getGraph

        catalog.store(targetGraph.qualifiedGraphName, resultGraph)

        CAPFResult.empty(innerResult.plans)

      case DeleteGraphStatement(_, targetGraph) =>
        catalog.delete(targetGraph.qualifiedGraphName)
        CAPFResult.empty()
    }

  }

  private def logStageProgress(s: String, newLine: Boolean = true): Unit = {
    if (PrintQueryExecutionStages.isSet) {
      if (newLine) {
        println(s)
      } else {
        val padded = s.padTo(30, " ").mkString("")
        print(padded)
      }
    }
  }

  override def filter(
    graph: PropertyGraph,
    in: CypherRecords,
    expr: Expr,
    queryParameters: CypherMap): CAPFRecords = {
    val scan = planStart(graph, in.asCapf.header.internalHeader.fields)
    val filter = producer.planFilter(expr, scan)
    planPhysical(in, queryParameters, filter).getRecords
  }

  override def select(
    graph: PropertyGraph,
    in: CypherRecords,
    fields: List[Var],
    queryParameters: CypherMap): CAPFRecords = {
    val scan = planStart(graph, in.asCapf.header.internalHeader.fields)
    val select = producer.planSelect(fields, scan)
    planPhysical(in, queryParameters, select).getRecords
  }

  override def project(
    graph: PropertyGraph,
    in: CypherRecords,
    expr: Expr,
    queryParameters: CypherMap): CAPFRecords = {
    val scan = planStart(graph, in.asCapf.header.internalHeader.fields)
    val project = producer.projectExpr(expr, scan)
    planPhysical(in, queryParameters, project).getRecords
  }

  override def alias(
    graph: PropertyGraph,
    in: CypherRecords,
    alias: (Expr, Var),
    queryParameters: CypherMap): CAPFRecords = {
    val (expr, v) = alias
    val scan = planStart(graph, in.asCapf.header.internalHeader.fields)
    val select = producer.projectField(IRField(v.name)(v.cypherType), expr, scan)
    planPhysical(in, queryParameters, select).getRecords
  }

  private def planCypherQuery(graph: PropertyGraph, cypherQuery: CypherQuery[Expr], allParameters: CypherMap, inputFields: Set[Var], drivingTable: CypherRecords) = {
    val LogicalPlan = planLogical(cypherQuery, graph, inputFields)
    planPhysical(drivingTable, allParameters, LogicalPlan)
  }

  private def planLogical(ir: CypherQuery[Expr], graph: PropertyGraph, inputFields: Set[Var]) = {
    logStageProgress("Logical planning ...", newLine = false)
    val logicalPlannerContext = LogicalPlannerContext(graph.schema, inputFields, catalog.listSources)
    val logicalPlan = time("Logical planning")(logicalPlanner(ir)(logicalPlannerContext))
    logStageProgress("Done!")
    if (PrintLogicalPlan.isSet) {
      println("Logical plan:")
      println(logicalPlan.pretty)
    }

    logStageProgress("Logical optimization ...", newLine = false)
    val optimizedLogicalPlan = time("Logical optimization")(logicalOptimizer(logicalPlan)(logicalPlannerContext))
    logStageProgress("Done!")
    if (PrintLogicalPlan.isSet) {
      println("Optimized logical plan:")
      println(optimizedLogicalPlan.pretty())
    }
    optimizedLogicalPlan
  }

  private def planPhysical(
    records: CypherRecords,
    parameters: CypherMap,
    logicalPlan: LogicalOperator,
    queryCatalog: QueryCatalog = QueryCatalog(catalog.listSources)
  ): CAPFResult = {
    logStageProgress("Flat planning ... ", newLine = false)
    val flatPlannerContext = FlatPlannerContext(parameters)
    val flatPlan = time("Flat planning")(flatPlanner(logicalPlan)(flatPlannerContext))
    logStageProgress("Done!")
    if (PrintFlatPlan.isSet) {
      println("Flat plan:")
      println(flatPlan.pretty())
    }

    logStageProgress("Physical planning ... ", newLine = false)
    val physicalPlannerContext = CAPFPhysicalPlannerContext.from(queryCatalog, records.asCapf, parameters)(self)
    val physicalPlan = time("Physical planning")(physicalPlanner(flatPlan)(physicalPlannerContext))
    logStageProgress("Done!")
    if (PrintPhysicalPlan.isSet) {
      println("Physical plan:")
      println(physicalPlan.pretty())
    }


    logStageProgress("Physical optimization ... ", newLine = false)
    val optimizedPhysicalPlan = time("Physical optimization")(physicalOptimizer(physicalPlan)(PhysicalOptimizerContext()))
    logStageProgress("Done!")
    if (PrintOptimizedPhysicalPlan.isSet) {
      println("Optimized physical plan:")
      println(optimizedPhysicalPlan.pretty())
    }

    val graphAt = (qgn: QualifiedGraphName) => Some(catalog.graph(qgn).asCapf)

    CAPFResultBuilder.from(logicalPlan, flatPlan, optimizedPhysicalPlan)(
      CAPFRuntimeContext(physicalPlannerContext.parameters, graphAt, collection.mutable.Map.empty, collection.mutable.Map.empty))
  }

  private[opencypher] val qgnGenerator = new QGNGenerator {
    override def generate: QualifiedGraphName = {
      QualifiedGraphName(SessionGraphDataSource.Namespace, GraphName(s"tmp#${maxSessionGraphId.incrementAndGet()}"))
    }
  }

  private def mountAmbientGraph(ambient: PropertyGraph): IRCatalogGraph = {
    val qgn = qgnGenerator.generate
    catalog.store(qgn, ambient)
    IRCatalogGraph(qgn, ambient.schema)
  }

  private def planStart(graph: PropertyGraph, fields: Set[Var]): LogicalOperator = {
    val ambientGraph = mountAmbientGraph(graph)

    producer.planStart(LogicalCatalogGraph(ambientGraph.qualifiedGraphName, graph.schema), fields)
  }

  override def sql(query: String): CAPFRecords =
    CAPFRecords.wrap(tableEnv.sqlQuery(query))
}
