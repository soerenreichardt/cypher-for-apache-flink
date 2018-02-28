package org.opencypher.flink.schema

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.opencypher.flink.schema.EntityTable.FlinkTable
import org.opencypher.flink.{CAPFRecords, CAPFSession, FlinkUtils}
import org.opencypher.flink.schema.Entity.sourceIdKey
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.CypherTable
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue


trait CAPFEntityTable extends EntityTable[FlinkTable] {
  private[flink] def entityType: CypherType with DefiniteCypherType = mapping.cypherType

  private[flink] def records(implicit capf: CAPFSession): CAPFRecords = CAPFRecords.create(this)
}

case class CAPFNodeTable(mapping: NodeMapping, table: FlinkTable) extends NodeTable(mapping, table) with CAPFEntityTable

object CAPFNodeTable {

  def apply(nodes: Table)(implicit capf: CAPFSession): CAPFNodeTable = {

    val labelsIndex = nodes.getSchema.columnNameToIndex("labels")
    val labels = nodes.toDataSet[Row]
      .map { _.getField(labelsIndex).asInstanceOf[Set[String]] }
        .collect().flatten.toSet


    val nodeMapping = NodeMapping.create(nodeIdKey = sourceIdKey, impliedLabels = labels, propertyKeys = Set("Properties"))
    print(nodeMapping)
    CAPFNodeTable(nodeMapping, nodes)
  }

}

case class CAPFRelationshipTable(mapping: RelationshipMapping, table: FlinkTable) extends RelationshipTable(mapping, table) with CAPFEntityTable

object CAPFRelationshipTable {

  def apply(rels: Table)(implicit capf: CAPFSession): CAPFRelationshipTable = {
    val relTypeIndex = rels.getSchema.columnNameToIndex("relType")
    val relType = rels.toDataSet[Row]
      .first(1)
      .map( _.getField(relTypeIndex).asInstanceOf[String])
      .collect().toSeq(0)
    val properties = rels.columns.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet

    val relationshipMapping = RelationshipMapping.create(
      sourceIdKey,
      Relationship.sourceStartNodeKey,
      Relationship.sourceEndNodeKey,
      relType,
      properties)

    print(relationshipMapping)
    CAPFRelationshipTable(relationshipMapping, rels)
  }
}

sealed trait EntityTable[T <: CypherTable[String]] {

  def schema: Schema

  def mapping: EntityMapping

  def table: T

  protected def verify(): Unit = {
    table.verifyColumnType(mapping.sourceIdKey, CTInteger, "id key")
  }

}

object EntityTable {

  implicit class FlinkTable(val table: Table) extends CypherTable[String] {

    override def columns: Seq[String] = table.getSchema.getColumnNames.toSeq

    override def columnType: Map[String, CypherType] = columns.map(c => c -> FlinkUtils.cypherTypeForColumn(table, c)).toMap

    /**
      * Iterator over the rows in this table.
      */
    override def rows: Iterator[String => CypherValue.CypherValue] = table.collect().iterator.map { row =>
      columns.map(c => c -> CypherValue(row.getField(table.getSchema.columnNameToIndex(c)))).toMap
    }

    /**
      * @return number of rows in this Table.
      */
    override def size: Long = table.count()
  }

}

abstract class NodeTable[T <: CypherTable[String]](mapping: NodeMapping, table: T) extends EntityTable[T] {

  override lazy val schema: Schema = {
    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
    }

    mapping.optionalLabelMapping.keys.toSet.subsets()
      .map(_.union(mapping.impliedLabels))
      .map(combo => Schema.empty.withNodePropertyKeys(combo.toSeq: _*)(propertyKeys: _*))
      .reduce(_ ++ _)
  }

  override protected def verify(): Unit = {
    super.verify()
    mapping.optionalLabelMapping.values.foreach( { optionalLabelKey =>
      table.verifyColumnType(optionalLabelKey, CTBoolean, "optional label")
    })
  }
}

abstract class RelationshipTable[T <: CypherTable[String]](mapping: RelationshipMapping, table: T) extends EntityTable[T] {

  override lazy val schema: Schema = {
    val relTypes = mapping.relTypeOrSourceRelTypeKey match {
      case Left(name) => Set(name)
      case Right((_, possibleTypes)) => possibleTypes
    }

    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
    }

    relTypes.foldLeft(Schema.empty) {
      case (partialSchema, relType) => partialSchema.withRelationshipPropertyKeys(relType)(propertyKeys: _*)
    }
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