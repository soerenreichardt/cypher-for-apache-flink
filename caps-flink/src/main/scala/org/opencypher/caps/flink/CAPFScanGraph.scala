package org.opencypher.caps.flink

import cats.data.NonEmptyVector
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.caps.flink.schema.{EntityTable, NodeTable, RelationshipTable}
import org.opencypher.caps.impl.record.RecordHeader
import org.opencypher.caps.ir.api.expr.Var

class CAPFScanGraph(val scans: Seq[EntityTable], val schema: Schema)(implicit val capf: CAPFSession)
  extends CAPFGraph {

  self: CAPFGraph =>

  private val nodeEntityTables = EntityTables(scans.collect { case it: NodeTable => it }.toVector)
  private val relEntityTables = EntityTables(scans.collect { case it: RelationshipTable => it }.toVector)

  override def nodes(name: String, nodeCypherType: CTNode): CAPFRecords = {
    val node = Var(name)(nodeCypherType)
    val selectedTables = nodeEntityTables.byType(nodeCypherType)
    val schema = selectedTables.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetNodeHeader = RecordHeader.nodeFromSchema(node, schema)

    val scanRecords: Seq[CAPFRecords] = selectedTables.map(_.records)
    val alignedRecords = scanRecords.map(_.alignWith(node, targetNodeHeader))
    alignedRecords.reduceOption(_ unionAll(targetNodeHeader, _)).getOrElse(CAPFRecords.empty(targetNodeHeader))
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPFRecords = {
    val rel = Var(name)(relCypherType)
    val selectedScans = relEntityTables.byType(relCypherType)
    val schema = selectedScans.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetRelHeader = RecordHeader.relationshipFromSchema(rel, schema)

    val scanRecords = selectedScans.map(_.records)
    val alignedRecords = scanRecords.map(_.alignWith(rel, targetRelHeader))
    alignedRecords.reduceOption(_ unionAll(targetRelHeader, _)).getOrElse(CAPFRecords.empty(targetRelHeader))
  }

  override def session: CAPFSession = ???

  override def union(other: PropertyGraph): CAPFGraph = ???

  case class EntityTables(entityTables: Vector[EntityTable]) {
    type EntityType = CypherType with DefiniteCypherType

    lazy val entityTableTypes: Vector[EntityType] = entityTables.map(_.entityType)

    lazy val entityTablesByType: Map[EntityType, NonEmptyVector[EntityTable]] =
      entityTables
          .groupBy(_.entityType)
          .flatMap { case (k, entityScans) => NonEmptyVector.fromVector(entityScans).map(k -> _) }

    def byType(entityType: EntityType): Seq[EntityTable] = {

      def isSubType(tableType: EntityType) = tableType.subTypeOf(entityType).isTrue

      val candidateTypes = entityTableTypes.filter(isSubType)
      val selectedScans = candidateTypes.flatMap(tpe => entityTablesByType.get(tpe).map(_.head))
      selectedScans
    }
  }
}
