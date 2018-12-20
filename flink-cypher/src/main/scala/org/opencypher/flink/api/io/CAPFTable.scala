package org.opencypher.flink.api.io

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions
import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink.impl.{CAPFRecords, RecordBehaviour}
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.types.CTString
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.relational.api.io.{EntityTable, NodeTable, RelationshipTable}
import org.opencypher.okapi.relational.api.table.RelationalEntityTableFactory

case object CAPFEntityTableFactory extends RelationalEntityTableFactory[FlinkTable] {
  override def nodeTable(
    nodeMapping: NodeMapping,
    table: FlinkTable
  ): NodeTable[FlinkTable] = {
    CAPFNodeTable(nodeMapping, table)
  }

  override def relationshipTable(
    relationshipMapping: RelationshipMapping,
    table: FlinkTable
  ): RelationshipTable[FlinkTable] = {
    CAPFRelationshipTable(relationshipMapping, table)
  }
}

trait CAPFEntityTable extends EntityTable[FlinkTable] {

  private[flink] def records(implicit capf: CAPFSession): CAPFRecords = capf.records.fromEntityTable(entityTable = this)
}

case class CAPFNodeTable(
  override val mapping: NodeMapping,
  override val table: FlinkTable
) extends NodeTable(mapping, table) with CAPFEntityTable with RecordBehaviour {

  override type Records = CAPFNodeTable

  override def cache(): CAPFNodeTable = {
    table.table.cache()
    this
  }
}

object CAPFNodeTable {

  def apply(impliedLabels: Set[String], nodeTable: Table): CAPFNodeTable =
    CAPFNodeTable(impliedLabels, Map.empty, nodeTable)

  def apply(impliedLabels: Set[String], optionalLabels: Map[String, String], nodeTable: Table): CAPFNodeTable = {
    val propertyColumnNames = properties(nodeTable.columns) -- optionalLabels.values

    val baseMapping = NodeMapping(GraphEntity.sourceIdKey, impliedLabels, optionalLabels)

    val nodeMapping = propertyColumnNames.foldLeft(baseMapping) { (mapping, propertyColumn) =>
      mapping.withPropertyKey(propertyColumn.toProperty, propertyColumn)
    }

    fromMapping(nodeMapping, nodeTable)
  }

  def fromMapping(mapping: NodeMapping, initialTable: Table): CAPFNodeTable = {
    val colsToSelect = mapping.allSourceKeys
    CAPFNodeTable(mapping, initialTable.select(colsToSelect.map(UnresolvedFieldReference): _*))
  }

  private def properties(nodeColumnNames: Seq[String]): Set[String] = {
    nodeColumnNames.filter(_ != GraphEntity.sourceIdKey).toSet
  }
}

case class CAPFRelationshipTable(
  override val mapping: RelationshipMapping,
  override val table: FlinkTable
) extends RelationshipTable(mapping, table) with CAPFEntityTable with RecordBehaviour {

  override type Records = CAPFRelationshipTable

  override def cache(): CAPFRelationshipTable = {
    table.table.cache()
    this
  }
}

object CAPFRelationshipTable {

  def apply(relationshipType: String, relationshipTable: Table): CAPFRelationshipTable = {
    val propertyColumnNames = properties(relationshipTable.columns)

    val baseMapping = RelationshipMapping.create(GraphEntity.sourceIdKey,
      Relationship.sourceStartNodeKey,
      Relationship.sourceEndNodeKey,
      relationshipType)

    val relationshipMapping = propertyColumnNames.foldLeft(baseMapping) { (mapping, propertyColumn) =>
      mapping.withPropertyKey(propertyColumn.toProperty, propertyColumn)
    }


    fromMapping(relationshipMapping, relationshipTable)
  }

  def fromMapping(mapping: RelationshipMapping, initialTable: Table): CAPFRelationshipTable = {

    val updatedTable = mapping.relTypeOrSourceRelTypeKey match {

      case Right((typeColumnName, relTypes)) =>
        FlinkTable(initialTable).verifyColumnType(typeColumnName, CTString, "relationship type")
        val updateTable = relTypes.foldLeft(initialTable) { case (currentTable, relType) =>
          val typeColumn = UnresolvedFieldReference(typeColumnName)
          val relTypeColumnName = relType.toRelTypeColumnName
          currentTable
            .safeUpsertColumn(relTypeColumnName, typeColumn === expressions.Literal(relType, Types.STRING))
        }
        updateTable.drop(typeColumnName).table

      case _ => initialTable
    }

    val colsToSelect = mapping.allSourceKeys

    CAPFRelationshipTable(mapping, updatedTable.select(colsToSelect.map(UnresolvedFieldReference): _*))
  }

  private def properties(relColumnNames: Seq[String]): Set[String] = {
    relColumnNames.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet
  }
}
