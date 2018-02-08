package org.opencypher.caps.flink

import com.sun.tools.javac.code.TypeTag
import org.apache.flink.table.api.Table
import org.opencypher.caps.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CypherType, DefiniteCypherType}
import org.opencypher.caps.flink.value.CAPFNode

sealed trait EntityTable {

  def schema: Schema

  def mapping: EntityMapping

  def table: Table

  private[capf] def entityType: CypherType with DefiniteCypherType = mapping.cypherType

  private[capf] def records(implicit capf: CAPFSession): CAPFRecords = CAPFRecords.create(this)

}

case class NodeTable(mapping: NodeMapping, table: Table)(implicit capf: CAPFSession) extends EntityTable {
  override lazy val schema: Schema = {
    val nodeDS = capf.tableEnv.toDataSet(table).getType.
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
  }

  private def properties(relColumnNames: Set[String]): Set[String] = {
    relColumnNames.filter(!Relationship.nonPropertyAttributes.contains(_))
  }
}
