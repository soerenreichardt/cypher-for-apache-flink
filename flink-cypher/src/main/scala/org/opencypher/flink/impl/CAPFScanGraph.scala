package org.opencypher.flink

import cats.data.NonEmptyVector
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{Literal, UnresolvedFieldReference}
import org.opencypher.flink.TableOps._
import org.opencypher.flink.api.io.{CAPFEntityTable, CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.okapi.api.io.conversion.RelationshipMapping
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.api.schema.RelationalSchema._

class CAPFScanGraph(val scans: Seq[CAPFEntityTable], val schema: CAPFSchema, val tags: Set[Int])(implicit val session: CAPFSession)
  extends CAPFGraph {

  self: CAPFGraph =>

  override def toString = s"CAPFScanGraph(${scans.map(_.entityType).mkString(",  ")})"

  private val nodeTables = scans.collect { case it: CAPFNodeTable => it }

  private val relTables = scans.collect { case it: CAPFRelationshipTable => it }

  override def nodes(name: String, nodeCypherType: CTNode): CAPFRecords =
    nodesInternal(name, nodeCypherType, byExactType = false)

  override def nodesWithExactLabels(name: String, labels: Set[String]): CAPFRecords =
    nodesInternal(name, CTNode(labels), byExactType = true)

  private def nodesInternal(name: String, nodeCypherType: CTNode, byExactType: Boolean): CAPFRecords = {
    val node = Var(name)(nodeCypherType)
    val selectedTables = if (byExactType) {
      nodeTables.filter(_.entityType == nodeCypherType)
    } else {
      nodeTables.filter(_.entityType.subTypeOf(nodeCypherType).isTrue)
    }
    val schema = selectedTables.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetNodeHeader = schema.headerForNode(node)

    alignRecords(selectedTables.map(_.records), node, targetNodeHeader).getOrElse(CAPFRecords.empty(targetNodeHeader))
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPFRecords = {
    val rel = Var(name)(relCypherType)
    val scanTypes = relCypherType.types
    val selectedScans = relTables.filter(relTable => scanTypes.isEmpty || scanTypes.exists(relTable.entityType.types.contains))
    val schema = selectedScans.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetRelHeader = schema.headerForRelationship(rel)

    val scanRecords = selectedScans.map(_.records)

    val filteredRecords = scanRecords
      .map { records =>
        val scanHeader = records.header
        val typeExprs = scanHeader
          .typesFor(Var("")(relCypherType))
          .filter(relType => relCypherType.types.contains(relType.relType.name))
          .toSeq

        typeExprs match {
          case Nil =>
            records
          case other =>
            val relTypeFilter = other
              .map(typeExpr => UnresolvedFieldReference(scanHeader.column(typeExpr)) === Literal(true, Types.BOOLEAN))
              .reduce(_ || _)
            CAPFRecords(scanHeader, records.table.filter(relTypeFilter))
        }
      }

    alignRecords(filteredRecords, rel, targetRelHeader).getOrElse(CAPFRecords.empty(targetRelHeader))
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
