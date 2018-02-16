package org.opencypher.caps.flink

import org.apache.flink.api.scala.DataSet
import org.apache.flink.table.api.Table
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.flink.CAPFConverters._
import org.opencypher.caps.flink.value.{CAPFNode, CAPFRelationship}
import org.opencypher.caps.impl.exception.IllegalArgumentException
import org.opencypher.caps.impl.table.{OpaqueField, RecordHeader}
import org.opencypher.caps.ir.api.expr.Var

trait CAPFGraph extends PropertyGraph with Serializable {

  override def nodes(name: String, nodeCypherType: CTNode): CAPFRecords

  override def nodes(name: String): CAPFRecords = nodes(name, CTNode)

  override def relationships(name: String, relCypherType: CTRelationship): CAPFRecords

  override def relationships(name: String): CAPFRecords = relationships(name, CTRelationship)

  override def session: CAPFSession

  override def union(other: PropertyGraph): CAPFGraph

  override def toString = s"${getClass.getSimpleName}"

}

object CAPFGraph {

  def empty(implicit capf: CAPFSession): CAPFGraph =
    new EmptyGraph() {

    }

  def create(nodes: Table, rels: Table)(implicit capf: CAPFSession) = {
    nodes.printSchema()
  }

  def createLazy(theSchema: Schema, loadGraph: => CAPFGraph)(implicit CAPF: CAPFSession): CAPFGraph =
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
