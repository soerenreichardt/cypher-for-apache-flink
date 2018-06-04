package org.opencypher.flink

import cats.data.NonEmptyVector
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{Literal, UnresolvedFieldReference}
import org.opencypher.flink.schema.{CAPFEntityTable, CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.TableOps._
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.io.conversion.RelationshipMapping
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader

class CAPFScanGraph(val scans: Seq[CAPFEntityTable], val schema: CAPFSchema, val tags: Set[Int])(implicit val session: CAPFSession)
  extends CAPFGraph {

  self: CAPFGraph =>

  override def toString = s"CAPFScanGraph(${scans.map(_.entityType).mkString(",  ")})"

  private val nodeEntityTables = EntityTables(scans.collect { case it: CAPFNodeTable => it })

  private val relEntityTables = EntityTables(scans.collect { case it: CAPFRelationshipTable => it })

  override def nodes(name: String, nodeCypherType: CTNode): CAPFRecords = {
    val node = Var(name)(nodeCypherType)
    val selectedTables = nodeEntityTables.byType(nodeCypherType)
    val schema = selectedTables.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetNodeHeader = RecordHeader.nodeFromSchema(node, schema)

    val scanRecords: Seq[CAPFRecords] = selectedTables.map(_.records)
    val alignedRecords = scanRecords.map(_.alignWith(node, targetNodeHeader))
    alignedRecords.reduceOption(_.unionAll(targetNodeHeader, _)).getOrElse(CAPFRecords.empty(targetNodeHeader))
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPFRecords = {
    val rel = Var(name)(relCypherType)
    val selectedScans = relEntityTables.byType(relCypherType)
    val schema = selectedScans.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetRelHeader = RecordHeader.relationshipFromSchema(rel, schema)

    val scanRecords: Seq[CAPFRecords] = selectedScans.map(_.records)
    val alignedRecords = scanRecords.map(_.alignWith(rel, targetRelHeader))
    alignedRecords.reduceOption(_ unionAll(targetRelHeader, _)).getOrElse(CAPFRecords.empty(targetRelHeader))
  }

  private class EntityTables(entityTables: Vector[CAPFEntityTable]) {

    type EntityType = CypherType with DefiniteCypherType

    lazy val entityTableTypes: Set[EntityType] = entityTables.map(_.entityType).toSet

    lazy val entityTablesByType: Map[EntityType, NonEmptyVector[CAPFEntityTable]] =
      entityTables
      .groupBy(_.entityType)
      .flatMap { case (k, entityScans) => NonEmptyVector.fromVector(entityScans).map(k -> _) }

    def byExactType(entityType: EntityType): Seq[CAPFEntityTable] = entityTablesByType(entityType).toVector

    def byType(entityType: EntityType): Seq[CAPFEntityTable] = {

      def isSubType(tableType: EntityType) = tableType.subTypeOf(entityType).isTrue

      entityTableTypes
        .filter(isSubType)
        .flatMap(typ => entityTablesByType(typ).toVector).toSeq
    }
  }

  private object EntityTables {

    def apply(entityTables: Seq[CAPFEntityTable]): EntityTables = new EntityTables(entityTables.flatMap {
      case CAPFRelationshipTable(relMapping@RelationshipMapping(_, _, _, Right((typeColumnName, relTypes)), _), flinkTable) =>
        val typeColumn = UnresolvedFieldReference(typeColumnName)
        relTypes.map {
          relType =>
            val filteredTable = flinkTable.table
              .filter(typeColumn === Literal(relType, Types.STRING))
              .safeDropColumn(typeColumnName)
            CAPFRelationshipTable.fromMapping(relMapping.copy(relTypeOrSourceRelTypeKey = Left(relType)), filteredTable)
        }
      case other => Seq(other)
    }.toVector)
  }
}
