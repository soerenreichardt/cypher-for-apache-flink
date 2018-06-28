package org.opencypher.flink.api.io

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.relational.api.io.{EntityTable, FlatRelationalTable}
import org.opencypher.flink.api.io.FlinkCypherTable.FlinkTable
import org.opencypher.flink.schema.CAPFSchema._
import org.opencypher.flink.impl.{CAPFRecords, CAPFSession, RecordBehaviour}
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink.impl.FlinkSQLExprMapper._
import org.opencypher.flink.schema.CAPFSchema
import org.opencypher.okapi.api.table.CypherTable._
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.relational.impl.physical._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.ir.api.expr.Expr

object FlinkCypherTable {

  implicit class FlinkTable(val table: Table) extends FlatRelationalTable[FlinkTable] {

    override def physicalColumns: Seq[String] = table.getSchema.getColumnNames

    override def columnType: Map[String, CypherType] = physicalColumns.map(c => c -> table.cypherTypeForColumn(c)).toMap

    override def rows: Iterator[String => CypherValue] = table.collect().iterator.map { row =>
      physicalColumns.map(c => c -> CypherValue(row.getField(table.getSchema.columnNameToIndex(c)))).toMap
    }

    override def size: Long = table.count()

    override def select(cols: String*): FlinkTable = {
      if (cols.nonEmpty) {
        table.select(cols.map(UnresolvedFieldReference): _*)
      } else {
        table.select()
      }
    }

    override def drop(cols: String*): FlinkTable = {
      val columnsLeft = table.physicalColumns.diff(cols)
      select(columnsLeft: _*)
    }

    override def orderBy(sortItems: (String, Order)*): FlinkTable = {
      val sortExpression = sortItems.map {
        case (column, Ascending) => UnresolvedFieldReference(column).asc
        case (column, Descending) => UnresolvedFieldReference(column).desc
      }

      table.orderBy(sortExpression: _*)
    }

    override def unionAll(other: FlinkTable): FlinkTable = {
      table.union(other.table)
    }

    override def join(other: FlinkTable, joinType: JoinType, joinCols: (String, String)*): FlinkTable = {

      val overlap = this.physicalColumns.toSet.intersect(other.physicalColumns.toSet)
      assert(overlap.isEmpty, s"overlapping columns: $overlap")

      val joinExpr = joinCols.map {
        case (l, r) => UnresolvedFieldReference(l) === UnresolvedFieldReference(r)
      }.foldLeft(Literal(true, Types.BOOLEAN): Expression) { (acc, expr) => acc && expr }

      joinType match {
        case InnerJoin => table.join(other.table, joinExpr)
        case LeftOuterJoin => table.leftOuterJoin(other.table, joinExpr)
        case RightOuterJoin => table.rightOuterJoin(other.table, joinExpr)
        case FullOuterJoin => table.fullOuterJoin(other.table, joinExpr)
        case CrossJoin => throw IllegalArgumentException("An implementation of cross that is called earlier.", "")
      }
    }

    override def distinct: FlinkTable =
      table.distinct()

    override def distinct(cols: String*): FlinkTable =
      table.distinct()

    override def withColumnRenamed(oldColumn: String, newColumn: String): FlinkTable =
      table.safeRenameColumn(oldColumn, newColumn)

    override def withNullColumn(col: String): FlinkTable = table.select('*, Null(Types.BOOLEAN) as Symbol(col))

    override def withTrueColumn(col: String): FlinkTable = table.select('*, Literal(true, Types.BOOLEAN) as Symbol(col))

    override def withFalseColumn(col: String): FlinkTable = table.select('*, Literal(false, Types.BOOLEAN) as Symbol(col))

    override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): FlinkTable = {
      table.filter(expr.asFlinkSQLExpr(header, table, parameters))
    }

    override def withColumn(column: String, expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): FlinkTable = {
      table.safeUpsertColumn(column, expr.asFlinkSQLExpr(header, table, parameters))
    }
  }

}

trait CAPFEntityTable extends EntityTable[FlinkTable] {

  private[flink] def entityType: CypherType with DefiniteCypherType

  private[flink] def records(implicit capf: CAPFSession): CAPFRecords = CAPFRecords.create(this)
}

case class CAPFNodeTable(
  override val mapping: NodeMapping,
  override val relationalTable: FlinkTable
) extends NodeTable(mapping, relationalTable) with CAPFEntityTable {

  override type R = CAPFNodeTable

  override def from(
    header: RecordHeader,
    table: FlinkTable,
    columnNames: Option[Seq[String]] = None): CAPFNodeTable = CAPFNodeTable(mapping, table)

  override private[flink] def entityType = mapping.cypherType

}

object CAPFNodeTable {

  def apply(impliedLabels: Set[String], nodeTable: Table): CAPFNodeTable =
    CAPFNodeTable(impliedLabels, Map.empty, nodeTable)

  def apply(impliedLabels: Set[String], optionalLabels: Map[String, String], nodeTable: Table): CAPFNodeTable = {
    val propertyColumnNames = properties(nodeTable.physicalColumns) -- optionalLabels.values

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

  private def properties(nodeColumnNames: Seq[String]): Set[String] =
    nodeColumnNames.filter(_ != GraphEntity.sourceIdKey).toSet
}

case class CAPFRelationshipTable(
  override val mapping: RelationshipMapping,
  override val relationalTable: FlinkTable
) extends RelationshipTable(mapping, relationalTable) with CAPFEntityTable {

  override type R = CAPFRelationshipTable

  override def from(
    header: RecordHeader,
    table: FlinkTable,
    columnNames: Option[Seq[String]] = None
  ): CAPFRelationshipTable = CAPFRelationshipTable(mapping, table)

  override private[flink] def entityType = mapping.cypherType
}

object CAPFRelationshipTable {

  def apply(relationshipType: String, relationshipTable: Table): CAPFRelationshipTable = {
    val propertyColumnNames = properties(relationshipTable.physicalColumns)

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
        val updatedTable = relTypes.foldLeft(initialTable) { case (currentTable, relType) =>
          val relTypeColumnName = relType.toRelTypeColumnName
          currentTable.safeAddColumn(relTypeColumnName, UnresolvedFieldReference(typeColumnName) === Literal(relType, Types.STRING))
        }
        updatedTable.safeDropColumn(typeColumnName)

      case _ => initialTable
    }

    val colsToSelect = mapping.allSourceKeys

    CAPFRelationshipTable(mapping, updatedTable.select(colsToSelect.map(UnresolvedFieldReference): _*))
  }

  private def properties(relColumnNames: Seq[String]): Set[String] = {
    relColumnNames.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet
  }
}

abstract class NodeTable(mapping: NodeMapping, table: FlinkTable) extends EntityTable[FlinkTable] with RecordBehaviour {

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

abstract class RelationshipTable(mapping: RelationshipMapping, table: FlinkTable) extends EntityTable[FlinkTable] with RecordBehaviour {

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
    mapping.relTypeOrSourceRelTypeKey.right.map { case (_, relTypes) =>
      relTypes.foreach { relType =>
        table.verifyColumnType(relType.toRelTypeColumnName, CTBoolean, "relationship type")
      }
    }
  }
}