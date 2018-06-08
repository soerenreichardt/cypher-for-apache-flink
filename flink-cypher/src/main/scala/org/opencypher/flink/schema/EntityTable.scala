package org.opencypher.flink.schema

import javassist.bytecode.stackmap.TypeTag

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.opencypher.flink.{CAPFRecords, CAPFSchema, CAPFSession, GraphEntity}
import org.opencypher.flink.TableOps._
import org.opencypher.flink.CAPFSchema._
import org.opencypher.flink.StringEncodingUtilities._
import org.opencypher.flink.schema.EntityTable.FlinkTable
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.CypherTable
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.okapi.impl.exception.IllegalArgumentException

sealed trait EntityTable[T <: CypherTable[String]] {

  verify()

  def schema: CAPFSchema

  def mapping: EntityMapping

  def table: T

  protected def verify(): Unit = {
    mapping.idKeys.foreach(key => table.verifyColumnType(key, CTInteger, "id key"))
    if (table.columns != mapping.allSourceKeys) throw IllegalArgumentException(
      s"Columns: ${mapping.allSourceKeys.mkString(", ")}",
      s"Columns: ${table.columns.mkString(", ")}",
      s"Use CAPF[Node|Relationship]Mapping#fromMapping to create a valid EntityTable")
  }
}

object EntityTable {

  implicit class FlinkTable(val table: Table) extends CypherTable[String] {

    override def columns: Seq[String] = table.getSchema.getColumnNames

    override def columnType: Map[String, CypherType] = columns.map(c => c -> table.cypherTypeForColumn(c)).toMap

    override def rows: Iterator[String => CypherValue] = table.collect().iterator.map { row =>
      columns.map(c => c -> CypherValue(row.getField(table.getSchema.columnNameToIndex(c)))).toMap
    }

    override def size: Long = table.count()

  }
}

trait CAPFEntityTable extends EntityTable[FlinkTable] {

  private[flink] def entityType: CypherType with DefiniteCypherType = mapping.cypherType

  private[flink] def records(implicit capf: CAPFSession): CAPFRecords = CAPFRecords.create(this)

}

case class CAPFNodeTable(mapping: NodeMapping, table: FlinkTable) extends NodeTable(mapping, table) with CAPFEntityTable

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

case class CAPFRelationshipTable(mapping: RelationshipMapping, table: FlinkTable) extends RelationshipTable(mapping, table) with CAPFEntityTable

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
    val colsToSelect = mapping.allSourceKeys
    CAPFRelationshipTable(mapping, initialTable.select(colsToSelect.map(UnresolvedFieldReference): _*))
  }

  private def properties(relColumnNames: Seq[String]): Set[String] = {
    relColumnNames.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet
  }
}

abstract class NodeTable[T <: CypherTable[String]](mapping: NodeMapping, table: T) extends EntityTable[T] {

  override lazy val schema: CAPFSchema = {
    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
    }

    mapping.optionalLabelMapping.keys.toSet.subsets
      .map(_.union(mapping.impliedLabels))
      .map(combo => Schema.empty.withNodePropertyKeys(combo.toSeq: _*)(propertyKeys: _*))
      .reduce(_ ++ _)
      .asCapf
  }

  override protected def verify(): Unit = {
    super.verify()
    mapping.optionalLabelMapping.values.foreach { optionalLabelKey =>
      table.verifyColumnType(optionalLabelKey, CTBoolean, "optional label")
    }
  }
}

abstract class RelationshipTable[T <: CypherTable[String]](
  mapping: RelationshipMapping,
  table: T
) extends EntityTable[T] {

  override lazy val schema: CAPFSchema = {
    val relTypes = mapping.relTypeOrSourceRelTypeKey match {
      case Left(name) => Set(name)
      case Right((_, possibleTypes)) => possibleTypes
    }

    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
    }

    relTypes.foldLeft(Schema.empty) {
      case (partialSchema, relType) => partialSchema.withRelationshipPropertyKeys(relType)(propertyKeys: _*)
    }.asCapf
  }

  override protected def verify(): Unit = {
    super.verify()
    table.verifyColumnType(mapping.sourceStartNodeKey, CTInteger, "start node")
    table.verifyColumnType(mapping.sourceEndNodeKey, CTInteger, "end node")
    mapping.relTypeOrSourceRelTypeKey.right.foreach { key =>
      table.verifyColumnType(key._1, CTString, "relationship type")
    }
  }
}