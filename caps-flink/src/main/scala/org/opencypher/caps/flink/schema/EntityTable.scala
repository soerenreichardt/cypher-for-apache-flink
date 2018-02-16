package org.opencypher.caps.flink.schema

import org.apache.flink.table.api.Table
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.opencypher.caps.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CypherType, _}
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.flink._
import org.opencypher.caps.impl.table.CypherTable

sealed trait EntityTable[T <: CypherTable[String]] {

  def schema: Schema

  def mapping: EntityMapping

  def table: T

  protected def verify(): Unit = {
    table.verifyColumnType(mapping.sourceIdKey, CTInteger, "id key")
  }

}

object EntityTable {

  implicit class FlinkTable(val table: Table)(implicit capf: CAPFSession) extends CypherTable[String] {

    override def columns: Seq[String] = table.getSchema.getColumnNames.toSeq

    override def columnType: Map[String, CypherType] = columns.map(c => c -> FlinkUtils.cypherTypeForColumn(table, c)).toMap

    /**
      * Iterator over the rows in this table.
      */
    override def rows: Iterator[String => CypherValue.CypherValue] = ???

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