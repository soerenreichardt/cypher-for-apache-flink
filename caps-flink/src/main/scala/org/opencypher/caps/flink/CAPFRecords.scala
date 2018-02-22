package org.opencypher.caps.flink

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.opencypher.caps.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.caps.flink.CAPFRecordHeader._
import org.opencypher.caps.flink.FlinkUtils._
import org.opencypher.caps.flink.schema.{CAPFEntityTable, CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.caps.flink.schema.EntityTable._
import org.opencypher.caps.impl.exception.{DuplicateSourceColumnException, IllegalArgumentException, IllegalStateException}
import org.opencypher.caps.impl.table._
import org.opencypher.caps.impl.util.PrintOptions
import org.opencypher.caps.ir.api.{Label, PropertyKey}
import org.opencypher.caps.ir.api.expr._

sealed abstract class CAPFRecords(val header: RecordHeader, val data: Table)(implicit capf: CAPFSession)
  extends CypherRecords with Serializable {

  override def print(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)

  override def register(name: String): Unit =
    capf.tableEnv.registerTable(name, data)

  override def size: Long = data.count()

  def toCypherMaps: DataSet[CypherMap] = {
    data.toDataSet[Row].map(rowToCypherMap(header))
  }

  override def columns: Seq[String] =
    header.fieldsInOrder

  override def rows: Iterator[String => CypherValue] =
    data.rows

  override def iterator: Iterator[CypherMap] =
    toCypherMaps.collect().iterator

  override lazy val columnType: Map[String, CypherType] = data.columnType

  def alignWith(v: Var, targetHeader: RecordHeader): Unit = {
    val oldEntity = this.header.internalHeader.fields.headOption
        .getOrElse(throw IllegalStateException("GraphScan table did not contain any fields"))
    val entityLabels: Set[String] = oldEntity.cypherType match {
      case CTNode(labels) => labels
      case CTRelationship(typ) => typ
      case _ => throw IllegalArgumentException("CTNode or CTRelationship", oldEntity.cypherType)
    }

    val slots = this.header.slots
    val renamedSlots = slots.map(_.withOwner(v))

    val dataColumnNameToIndex: Map[String, Int] = renamedSlots.map { dataSlot =>
      val dataColumnName = ColumnName.of(dataSlot)
      val dataColumnIndex = dataSlot.index
      dataColumnName -> dataColumnIndex
    }.toMap

    val slotDataSelector: Seq[Row => Any] = targetHeader.slots.map { targetSlot =>
      val columnName = ColumnName.of(targetSlot)
      val defaultValue = targetSlot.content.key match {
        case HasLabel(_, l: Label) => entityLabels(l.name)
        case _: Type if entityLabels.size == 1 => entityLabels.head
        case _ => null
      }
      val maybeDataIndex = dataColumnNameToIndex.get(columnName)
      val slotDataSelector: Row => Any = maybeDataIndex match {
        case None =>
          (_) =>
            defaultValue
        case Some(index) => _.getField(index)
      }
      slotDataSelector
    }
    val wrappedHeader = new CAPFRecordHeader(targetHeader)
    println("Wrapped Header: " + wrappedHeader)

  }

  def toTable(): Table = data
}

object CAPFRecords extends CypherRecordsCompanion[CAPFRecords, CAPFSession] {

  private[flink] val placeHolderVarName = ""

  def create(entityTable: CAPFEntityTable)(implicit capf: CAPFSession): CAPFRecords = {
    def sourceColumnToPropertyExpressionMapping(variable: Var): Map[String, Expr] = {
      val keyMap = entityTable.mapping.propertyMapping.map {
        case (key, sourceColumn) => sourceColumn -> Property(variable, PropertyKey(key))()
      }.foldLeft(Map.empty[String, Expr]) {
        case (m, (sourceColumn, propertyExpr)) =>
          if (m.contains(sourceColumn))
            throw DuplicateSourceColumnException(sourceColumn, variable)
          else m.updated(sourceColumn, propertyExpr)
      }
      val sourceKey = entityTable.mapping.sourceIdKey
      if (keyMap.contains(sourceKey))
        throw DuplicateSourceColumnException(sourceKey, variable)
      else keyMap.updated(sourceKey, variable)
    }

    def sourceColumnNodeToExpressionMapping(nodeMapping: NodeMapping): Map[String, Expr] = {
      val generatedVar = Var(placeHolderVarName)(nodeMapping.cypherType)

      val entityMappings = sourceColumnToPropertyExpressionMapping(generatedVar)

      nodeMapping.optionalLabelMapping.map {
        case (label, sourceColumn) =>  {
          sourceColumn -> HasLabel(generatedVar, Label(label))(CTBoolean)
        }
      }.foldLeft(entityMappings) {
        case (m, (sourceColumn, expr)) =>
          if (m.contains(sourceColumn))
            throw DuplicateSourceColumnException(sourceColumn, generatedVar)
          else m.updated(sourceColumn, expr)
      }
    }

    def sourceColumnRelationshipToExpressionMapping(relMapping: RelationshipMapping): Map[String, Expr] = {
      val relVar = Var(placeHolderVarName)(relMapping.cypherType)
      val entityMappings = sourceColumnToPropertyExpressionMapping(relVar)

      val sourceColumnToExpressionMapping: Map[String, Expr] = Seq(
        relMapping.sourceStartNodeKey -> StartNode(relVar)(CTInteger),
        relMapping.sourceEndNodeKey -> EndNode(relVar)(CTInteger)
      ).foldLeft(entityMappings) {
        case (acc, (slot, expr)) =>
          if (acc.contains(slot))
            throw DuplicateSourceColumnException(slot, relVar)
          else acc.updated(slot, expr)
      }
      relMapping.relTypeOrSourceRelTypeKey match {
        case Right((sourceRelTypeColumn, _)) if sourceColumnToExpressionMapping.contains(sourceRelTypeColumn) =>
          throw DuplicateSourceColumnException(sourceRelTypeColumn, relVar)
        case Right((sourceRelTypeColumn, _)) => sourceColumnToExpressionMapping.updated(sourceRelTypeColumn, Type(relVar)(CTString))
        case Left(_) => sourceColumnToExpressionMapping
      }
    }

    val sourceColumnToExpressionMap = entityTable match {
      case nt: CAPFNodeTable => sourceColumnNodeToExpressionMapping(nt.mapping)
      case rt: CAPFRelationshipTable => sourceColumnRelationshipToExpressionMapping(rt.mapping)
    }

    val (sourceHeader, sourceTable) = {
      prepareDataFrame(entityTable.table.table)
    }

    val slotContents: Seq[SlotContent] = sourceHeader.slots.map {
      case slot@RecordSlot(_, content: FieldSlotContent) =>
        def sourceColumnName = content.field.name

        val expressionForColumn = sourceColumnToExpressionMap.get(sourceColumnName)
        expressionForColumn.map {
          case expr: Var => OpaqueField(expr)
          case expr: Property => ProjectedExpr(expr.copy()(cypherType = content.cypherType))
          case expr => ProjectedExpr(expr)
        }.getOrElse(slot.content)

      case slot =>
        slot.content
    }
    val newHeader = RecordHeader.from(slotContents: _*)
    val renamed = sourceTable.as(newHeader.internalHeader.columns.mkString(","))

    CAPFRecords.createInternal(newHeader, renamed)
  }

  private def prepareDataFrame(initialTable: Table)(implicit capf: CAPFSession): (RecordHeader, Table) = {
    // TODO: cast to compatible types
    val initialHeader = CAPFRecordHeader.fromFlinkTableSchema(initialTable.getSchema)
    val withRenamedColumns = initialTable.as(initialHeader.internalHeader.columns.mkString(","))
    (initialHeader, withRenamedColumns)
  }

  def empty(initialHeader: RecordHeader = RecordHeader.empty)(implicit capf: CAPFSession): CAPFRecords = {
    val nameToFlinkTypeMapping = initialHeader.internalHeader.slots.map(slot => ColumnName.of(slot) -> toFlinkType(slot.content.cypherType))

    //    TODO: rename columns and set correct datatypes
    val emptyDataSet = capf.env.fromCollection(Set.empty[Any])
    val initialTable = capf.tableEnv.fromDataSet(emptyDataSet)
    createInternal(initialHeader, initialTable)
  }

  override def unit()(implicit capf: CAPFSession): CAPFRecords = {
    val initialTable = capf.tableEnv.fromDataSet(capf.env.fromCollection(Seq(EmptyRow())))
    createInternal(RecordHeader.empty, initialTable)
  }

  private def createInternal(header: RecordHeader, data: Table)(implicit capf: CAPFSession) =
    new CAPFRecords(header, data) {}

  private case class EmptyRow()
}