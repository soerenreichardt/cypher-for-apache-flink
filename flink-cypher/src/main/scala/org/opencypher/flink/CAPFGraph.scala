package org.opencypher.flink

import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{Expression, Literal, UnresolvedFieldReference}
import org.opencypher.flink.CAPFConverters._
import org.opencypher.flink.CAPFSchema._
import org.opencypher.flink.schema.{CAPFEntityTable, CAPFNodeTable}
import org.opencypher.okapi.api.graph.{GraphOperations, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr.{Property, Var}
import org.opencypher.okapi.relational.impl.table.{OpaqueField, RecordHeader}

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

    val idSlot = records.header.slotFor(nodeVar)

    val labelSlots = records.header.labelSlots(nodeVar)
      .filter(slot => labels.contains(slot._1.label.name))
      .values

    val propertyExprs = schema.nodeKeys(labels).flatMap {
      case (key, cypherType) => Property(nodeVar, PropertyKey(key))(cypherType)
    }.toSet
    val propertySlots = records.header.propertySlots(nodeVar).filter {
      case (_, propertySlot) => propertyExprs.contains(propertySlot.content.key)
    }.values

    val keepSlots = (Seq(idSlot) ++ labelSlots ++ propertySlots).map(_.content)
    val keepCols = keepSlots.map(ColumnName.of)

    val predicate = records.header.labelSlots(nodeVar)
      .filterNot(slot => labels.contains(slot._1.label.name))
      .values
      .foldLeft(Literal(true, Types.BOOLEAN): Expression) { (acc, slot) =>
          acc && (UnresolvedFieldReference(ColumnName.of(slot)) === false)
      }

    val updatedData = records.data.filter(predicate).select(keepCols.map(UnresolvedFieldReference): _*)
//    val updatedData = predicate match {
//
//      case Some(filter) =>
//        records.data
//          .filter(filter)
//          .select(keepCols.map(UnresolvedFieldReference): _*)
//
//      case None =>
//        records.data.select(keepCols.map(UnresolvedFieldReference): _*)
//    }

    val updatedHeader = RecordHeader.from(keepSlots: _*)

    CAPFRecords.verifyAndCreate(updatedHeader, updatedData)(session)
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

  def createLazy(theSchema: CAPFSchema, loadGraph: => CAPFGraph)(implicit capf: CAPFSession): CAPFGraph =
    new LazyGraph(theSchema, loadGraph) {}

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
      CAPFRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def relationships(name: String, cypherType: CTRelationship): CAPFRecords =
      CAPFRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

  }

}
