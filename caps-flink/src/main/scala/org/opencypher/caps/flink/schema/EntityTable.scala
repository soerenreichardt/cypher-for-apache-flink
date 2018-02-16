package org.opencypher.caps.flink.schema

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.types.Row
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CypherType, DefiniteCypherType}
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.flink.FlinkUtils._
import org.opencypher.caps.flink.{Annotation, _}
import org.opencypher.caps.impl.record.CypherTable
import org.apache.flink.table.api.scala._
import org.opencypher.caps.api.exception.IllegalArgumentException

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

sealed trait EntityTable[T <: CypherTable] {

  def schema: Schema

  def mapping: EntityMapping

  def table: T

  protected def verify(): Unit = {
    val sourceIdKeyType = table.columnType(mapping.sourceIdKey)
    if (sourceIdKeyType != CTInteger) throw IllegalArgumentException(
      s"id key type in column `${mapping.sourceIdKey}` that is compatible with CTInteger", sourceIdKeyType)
  }

}

object EntityTable {

  implicit class FlinkTable(val table: Table)(implicit capf: CAPFSession) extends CypherTable {

    override def columns: Set[String] = table.getSchema.getColumnNames.toSet

    override def columnType: Map[String, CypherType] = columns.map(c => c -> FlinkUtils.cypherTypeForColumn(table, c)).toMap

    /**
      * Iterator over the rows in this table.
      */
    override def rows: Iterator[String => CypherValue.CypherValue] = ???

    /**
      * @return number of rows in this Table.
      */
    override def size: Long = ???
  }

}

abstract class NodeTable[T <: CypherTable](mapping: NodeMapping, table: T) extends EntityTable[T] {

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
    mapping.optionalLabelMapping.values.foreach { optionalLabelKey =>
      val columnType = table.columnType(optionalLabelKey)
      if (columnType != CTBoolean) {
        throw IllegalArgumentException(
          s"optional label key type in column `$optionalLabelKey`that is compatible with CTBoolean", columnType)
      }
    }
  }
}

abstract class RelationshipTable[T <: CypherTable](mapping: RelationshipMapping, table: T) extends EntityTable[T] {

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

    val sourceStartNodeKeyType = table.columnType(mapping.sourceStartNodeKey)
    if (sourceStartNodeKeyType != CTInteger) throw IllegalArgumentException(
      s"start node key type in column `${mapping.sourceStartNodeKey}` that is compatible with CTInteger", sourceStartNodeKeyType)

    val sourceEndNodeKeyType = table.columnType(mapping.sourceEndNodeKey)
    if (sourceEndNodeKeyType != CTInteger) throw IllegalArgumentException(
      s"end node key type in column `${mapping.sourceEndNodeKey}` that is compatible with CTInteger", sourceEndNodeKeyType)

    mapping.relTypeOrSourceRelTypeKey.right.foreach { k =>
      val relTypeKey = k._1
      val relType = table.columnType(relTypeKey)
      if (relType != CTString) throw IllegalArgumentException(
        s"relationship type in column `${relTypeKey}` that is compatible with CTString", relType)
    }
  }
}