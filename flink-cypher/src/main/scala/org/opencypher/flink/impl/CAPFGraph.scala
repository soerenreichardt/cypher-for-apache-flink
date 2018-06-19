package org.opencypher.flink.impl

import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{Expression, Literal, UnresolvedFieldReference}
import org.opencypher.flink.api.io.{CAPFEntityTable, CAPFNodeTable}
import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.flink.schema.CAPFSchema
import org.opencypher.flink.schema.CAPFSchema._
import org.opencypher.okapi.api.graph.{GraphOperations, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr.{Expr, Property, Var}
import org.opencypher.okapi.relational.impl.table.RecordHeader

trait CAPFGraph extends PropertyGraph with GraphOperations with Serializable {

  def tags: Set[Int]

  implicit def session: CAPFSession

  override def nodes(name: String, nodeCypherType: CTNode): CAPFRecords

  override def relationships(name: String, relCypherType: CTRelationship): CAPFRecords

  override def unionAll(others: PropertyGraph*): CAPFGraph = {
    CAPFUnionGraph(this :: others.map(_.asCapf).toList: _*)
  }

  override def schema: CAPFSchema

  override def toString = s"${getClass.getSimpleName}"

  def nodesWithExactLabels(name: String, labels: Set[String]): CAPFRecords = {
    val nodeType = CTNode(labels)
    val nodeVar = Var(name)(nodeType)
    val records = nodes(name, nodeType)

    val header = records.header

    val idColumn = records.header.column(nodeVar)

    val labelExprs = records.header.labelsFor(nodeVar)

    val labelColumns = labelExprs.map(header.column)

    val propertyExprs = schema.nodeKeys(labels).flatMap {
      case (key, cypherType) => Property(nodeVar, PropertyKey(key))(cypherType)
    }.toSet
    val headerPropertyExprs = header.propertiesFor(nodeVar).filter(propertyExprs.contains)

    val keepExprs: Seq[Expr] = Seq(nodeVar) ++ labelExprs ++ headerPropertyExprs

    val keepColumns = keepExprs.map(header.column)

    val predicate = labelExprs
      .filterNot(l => labels.contains(l.label.name))
      .map(header.column)
      .foldLeft(Literal(true, Types.BOOLEAN): Expression) { (acc, columnName) =>
          acc && (UnresolvedFieldReference(columnName) === false)
      }

    val updatedData = records.table
          .filter(predicate)
          .select(keepColumns.map(UnresolvedFieldReference): _*)

    val updatedHeader = RecordHeader.from(keepExprs)

    CAPFRecords(updatedHeader, updatedData)(session)
  }

  protected def alignRecords(records: Seq[CAPFRecords], targetVar: Var, targetHeader: RecordHeader): Option[CAPFRecords] = {
    val alignedRecords = records.map(_.alignWith(targetVar, targetHeader))
    val selectExpressions = targetHeader.expressions.toSeq.sorted
    val consistentRecords = alignedRecords.map(_.select(selectExpressions.head, selectExpressions.tail: _*))
    consistentRecords.reduceOption(_ unionAll _)
  }

}

object CAPFGraph {

  def empty(implicit capf: CAPFSession): CAPFGraph =
    new EmptyGraph() {
      override def tags: Set[Int] = Set.empty
    }

  def create(nodeTable: CAPFNodeTable, entityTables: CAPFEntityTable*)(implicit capf: CAPFSession): CAPFGraph = {
    create(Set(0), nodeTable, entityTables: _*)
  }

  def create(tags: Set[Int], nodeTable: CAPFNodeTable, entityTables: CAPFEntityTable*)(implicit capf: CAPFSession): CAPFGraph = {
    val allTables = nodeTable +: entityTables
    val schema = allTables.map(_.schema).reduce[Schema](_ ++ _).asCapf
    new CAPFScanGraph(allTables, schema, tags)
  }

  def create(records: CypherRecords, schema: Schema, tags: Set[Int] = Set(0))(implicit capf: CAPFSession): CAPFGraph = {
    val capfRecords = records.asCapf
    ???
//    new CAPFPatternGraph(capfRecords, schema, tags)
  }

  sealed abstract class LazyGraph(override val schema: CAPFSchema, loadGraph: => CAPFGraph)(implicit CAPF: CAPFSession)
    extends CAPFGraph {
    protected lazy val lazyGraph: CAPFGraph = {
      val g = loadGraph
      if (g.schema == schema) g else throw IllegalArgumentException(s"a graph with schema $schema", g.schema)
    }

    override def tags: Set[Int] = lazyGraph.tags

    override def session: CAPFSession = CAPF

    override def nodes(name: String, nodeCypherType: CTNode): CAPFRecords =
      lazyGraph.nodes(name, nodeCypherType)

    override def relationships(name: String, relCypherType: CTRelationship): CAPFRecords =
      lazyGraph.relationships(name, relCypherType)

  }

  sealed abstract class EmptyGraph(implicit val session: CAPFSession) extends CAPFGraph {

    override val schema: CAPFSchema = CAPFSchema.empty

    override def nodes(name: String, cypherType: CTNode): CAPFRecords =
      CAPFRecords.empty(RecordHeader.from(Var(name)(cypherType)))

    override def relationships(name: String, cypherType: CTRelationship): CAPFRecords =
      CAPFRecords.empty(RecordHeader.from(Var(name)(cypherType)))

  }

}
