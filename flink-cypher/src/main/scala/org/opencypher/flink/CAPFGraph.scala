package org.opencypher.flink

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.opencypher.flink.schema.EntityTable._
import org.opencypher.flink.schema.{CAPFEntityTable, CAPFNodeTable}
import org.opencypher.flink.CAPFConverters._
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.{OpaqueField, RecordHeader}

trait CAPFGraph extends PropertyGraph with Serializable {

  override def nodes(name: String, nodeCypherType: CTNode): CAPFRecords

  override def relationships(name: String, relCypherType: CTRelationship): CAPFRecords

  override def session: CAPFSession

  override def union(other: PropertyGraph): CAPFGraph

  override def toString = s"${getClass.getSimpleName}"

}

object CAPFGraph {

  def empty(implicit capf: CAPFSession): CAPFGraph =
    new EmptyGraph() {

    }

  def create(nodeTable: CAPFNodeTable, entityTables: CAPFEntityTable*)(implicit capf: CAPFSession): CAPFGraph = {
    val allTables = nodeTable +: entityTables
    val schema = allTables.map(_.schema).reduce(_ ++ _)
    new CAPFScanGraph(allTables, schema)
  }

  def create(records: CypherRecords, schema: Schema)(implicit capf: CAPFSession): CAPFGraph = {
    ???
  }

  def createLazy(theSchema: Schema, loadGraph: => CAPFGraph)(implicit capf: CAPFSession): CAPFGraph =
    new LazyGraph(theSchema, loadGraph) {}

  sealed abstract class LazyGraph(override val schema: Schema, loadGraph: => CAPFGraph)(implicit CAPF: CAPFSession)
    extends CAPFGraph {
    protected lazy val lazyGraph: CAPFGraph = {
      val g = loadGraph
      if (g.schema == schema) g else throw IllegalArgumentException(s"a graph with schema $schema", g.schema)
    }

    override def session: CAPFSession = CAPF

    override def nodes(name: String, nodeCypherType: CTNode): CAPFRecords =
      lazyGraph.nodes(name, nodeCypherType)

    override def relationships(name: String, relCypherType: CTRelationship): CAPFRecords =
      lazyGraph.relationships(name, relCypherType)

    override def union(other: PropertyGraph): CAPFGraph =
      lazyGraph.union(other)

  }

  sealed abstract class EmptyGraph(implicit val session: CAPFSession) extends CAPFGraph {

    override val schema: Schema = Schema.empty

    override def nodes(name: String, cypherType: CTNode): CAPFRecords =
      CAPFRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def relationships(name: String, cypherType: CTRelationship): CAPFRecords =
      CAPFRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def union(other: PropertyGraph): CAPFGraph = other.asCapf
  }

}
