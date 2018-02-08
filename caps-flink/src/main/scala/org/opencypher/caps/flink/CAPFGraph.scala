package org.opencypher.caps.flink

import org.opencypher.caps.api.exception.IllegalArgumentException
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.flink.value.{CAPFNode, CAPFRelationship}
import org.opencypher.caps.impl.record.{CypherRecords, OpaqueField, RecordHeader}
import org.opencypher.caps.ir.api.expr.Var
trait CAPFGraph extends PropertyGraph with Serializable {

  override def nodes(name: String, nodeCypherType: CTNode): CAPFRecords

  override def nodes(name: String): CAPFRecords = nodes(name, CTNode)

  override def relationships(name: String, relCypherType: CTRelationship): CAPFRecords

  override def relationships(name: String): CAPFRecords = relationships(name, CTRelationship)

  override def session: CAPFSession

  override def union(other: PropertyGraph): CAPFGraph

  def cache(): CAPFGraph

  def persist(): CAPFGraph

  def persist(storageLevel: StorageLevel): CAPFGraph

  def unpersist(): CAPFGraph

  def unpersist(blocking: Boolean): CAPFGraph

  override def toString = s"${getClass.getSimpleName}"

}

object CAPFGraph {

  def empty(implicit capf: CAPFSession): CAPFGraph =
    new EmptyGraph() {

      override def session: CAPFSession = capf

      override def cache(): CAPFGraph = this

      override def persist(): CAPFGraph = this

      override def persist(storageLevel: StorageLevel): CAPFGraph = this

      override def unpersist(): CAPFGraph = this

      override def unpersist(blocking: Boolean): CAPFGraph = this
    }

  def create(nodeTable: NodeTable, entityTables: EntityTable*)(implicit capf: CAPFSession): CAPFGraph = {
    val allTables = nodeTable +: entityTables
    val schema = allTables.map(_.schema).reduce(_ ++ _)
    new CAPFScanGraph(allTables, schema)
  }

  def create(records: CypherRecords, schema: Schema)(implicit capf: CAPFSession): CAPFGraph = {
    val CAPFRecords = records.asCAPF
    new CAPFPatternGraph(CAPFRecords, schema)
  }

  def create(nodes: Seq[CAPFNode], rels: Seq[CAPFRelationship])(implicit capf: CAPFSession) = {
    val nodeDS = capf.env.fromCollection(nodes)
    val relDS = capf.env.fromCollection(rels)


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

    override def cache(): CAPFGraph = {
      lazyGraph.cache(); this
    }

    override def persist(): CAPFGraph = {
      lazyGraph.persist(); this
    }

    override def persist(storageLevel: StorageLevel): CAPFGraph = {
      lazyGraph.persist(storageLevel)
      this
    }

    override def unpersist(): CAPFGraph = {
      lazyGraph.unpersist(); this
    }

    override def unpersist(blocking: Boolean): CAPFGraph = {
      lazyGraph.unpersist(blocking); this
    }
  }

  sealed abstract class EmptyGraph(implicit val session: CAPFSession) extends CAPFGraph {

    override val schema: Schema = Schema.empty

    override def nodes(name: String, cypherType: CTNode): CAPFRecords =
      CAPFRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def relationships(name: String, cypherType: CTRelationship): CAPFRecords =
      CAPFRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def union(other: PropertyGraph): CAPFGraph = other.asCAPF
  }

}
