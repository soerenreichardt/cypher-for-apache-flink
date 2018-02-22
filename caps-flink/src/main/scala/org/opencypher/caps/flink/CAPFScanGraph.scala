package org.opencypher.caps.flink

import cats.data.NonEmptyVector
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.caps.flink.schema.{CAPFEntityTable, CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.caps.impl.table.RecordHeader
import org.opencypher.caps.ir.api.expr.Var

class CAPFScanGraph(val scans: Seq[CAPFEntityTable], val schema: Schema)(implicit val session: CAPFSession)
  extends CAPFGraph {

  self: CAPFGraph =>

  private val nodeEntityTables = EntityTables(scans.collect { case it: CAPFNodeTable => it }.toVector)
  private val relEntityTables = EntityTables(scans.collect { case it: CAPFRelationshipTable => it }.toVector)

  override def nodes(name: String, nodeCypherType: CTNode): CAPFRecords = {
    val node = Var(name)(nodeCypherType)
    val selectedTables = nodeEntityTables.byType(nodeCypherType)
    val schema = selectedTables.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetNodeHeader = RecordHeader.nodeFromSchema(node, schema)

    val scanRecords: Seq[CAPFRecords] = selectedTables.map(_.records)
    println(scanRecords.map(_.print))
    println(targetNodeHeader.slots.map( _.content))
    val alignedRecords = scanRecords.map(_.alignWith(node, targetNodeHeader))
//    alignedRecords.reduceOption(_ unionAll(targetNodeHeader, _)).getOrElse(CAPFRecords.empty(targetNodeHeader))
    CAPFRecords.empty()
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPFRecords = {
    val rel = Var(name)(relCypherType)
    val selectedScans = relEntityTables.byType(relCypherType)
    val schema = selectedScans.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetRelHeader = RecordHeader.relationshipFromSchema(rel, schema)

    val scanRecords = selectedScans.map(_.records)
    val alignedRecords = scanRecords.map(_.alignWith(rel, targetRelHeader))

    CAPFRecords.empty()
  }


  override def union(other: PropertyGraph): CAPFGraph = ???

  case class EntityTables(entityTables: Vector[CAPFEntityTable]) {
    type EntityType = CypherType with DefiniteCypherType

    lazy val entityTableTypes: Vector[EntityType] = entityTables.map(_.entityType)

    lazy val entityTablesByType: Map[EntityType, NonEmptyVector[CAPFEntityTable]] =
      entityTables
      .groupBy(_.entityType)
      .flatMap { case (k, entityScans) => NonEmptyVector.fromVector(entityScans).map(k -> _) }

    def byType(entityType: EntityType): Seq[CAPFEntityTable] = {

      def isSubType(tableType: EntityType) = tableType.subTypeOf(entityType).isTrue

      val candidateTypes = entityTableTypes.filter(isSubType)
      val selectedScans = candidateTypes.flatMap(typ => entityTablesByType.get(typ).map(_.head))
      selectedScans
    }
  }
}
