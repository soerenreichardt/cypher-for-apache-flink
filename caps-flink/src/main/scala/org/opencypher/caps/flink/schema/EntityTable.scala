package org.opencypher.caps.flink.schema

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row
import org.opencypher.caps.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CypherType, DefiniteCypherType}
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.flink.FlinkUtils._
import org.opencypher.caps.flink.{Annotation, _}
import org.opencypher.caps.impl.record.CypherTable

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

sealed trait EntityTable {

  def schema: Schema

  def mapping: EntityMapping

  def table: Table

  private[caps] def entityType: CypherType with DefiniteCypherType = mapping.cypherType

  private[caps] def records(implicit capf: CAPFSession): CAPFRecords = CAPFRecords.create(this)

}

object EntityTable {

  implicit class FlinkTable(val table: Table) extends CypherTable {

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

case class NodeTable(mapping: NodeMapping, table: Table)(implicit capf: CAPFSession) extends EntityTable {
  override lazy val schema: Schema = {
    val nodeDS = capf.tableEnv.toDataSet[Row](table)
    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> cypherTypeForColumn(table, sourceKey)
    }

    mapping.optionalLabelMapping.keys.toSet.subsets
        .map(_.union(mapping.impliedLabels))
        .map(combo => Schema.empty.withNodePropertyKeys(combo.toSeq: _*)(propertyKeys: _*))
        .reduce(_ ++ _)
  }
}

object NodeTable {

  def apply[E <: Node : TypeTag](nodes: Seq[E])(implicit capf: CAPFSession): NodeTable = {
    val nodeLabels = Annotation.labels[E]
    val nodeDS = capf.env.fromCollection(nodes)
    val nodeTable = capf.tableEnv.fromDataSet(nodeDS)
    val nodeProperties = properties(nodeTable.getSchema.getColumnNames)
    val nodeMapping = NodeMapping.create(nodeIdKey = Entity.sourceIdKey, impliedLabels = nodeLabels, propertyKeys = nodeProperties)
    NodeTable(nodeMapping, nodeTable)
  }

  private def properties(nodeColumnNames: Seq[String]): Set[String] = {
    nodeColumnNames.filter(_ != Entity.sourceIdKey).toSet
  }
}

case class RelationshipTable(mapping: RelationshipMapping, table: Table) extends EntityTable {
  override lazy val schema: Schema = {
    val relTypes = mapping.relTypeOrSourceRelTypeKey match {
      case Left(name) => Set(name)
      case Right((_, possibleTypes)) => possibleTypes
    }

    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> cypherTypeForColumn(table, sourceKey)
    }

    relTypes.foldLeft(Schema.empty) {
      case (partialSchema, relType) => partialSchema.withRelationshipPropertyKeys(relType)(propertyKeys: _*)
    }
  }
}

object RelationshipTable {

  def apply[E <: Relationship : TypeTag](relationships: Seq[E])(implicit capf: CAPFSession): RelationshipTable = {
    val relationshipType: String = Annotation.relType[E]
    val relationshipDS = capf.env.fromCollection(relationships)
    val relationshipTable = capf.tableEnv.fromDataSet(relationshipDS)
    val relationshipProperties = properties(relationshipTable.getSchema.getColumnNames.toSet)

    val relationshipMapping = RelationshipMapping.create(Entity.sourceIdKey,
      Relationship.sourceStartNodeKey,
      Relationship.sourceEndNodeKey,
      relationshipType,
      relationshipProperties)

    RelationshipTable(relationshipMapping, relationshipTable)
  }

  private def properties(relColumnNames: Set[String]): Set[String] = {
    relColumnNames.filter(!Relationship.nonPropertyAttributes.contains(_))
  }
}
