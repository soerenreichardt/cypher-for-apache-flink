package org.opencypher.caps.flink

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row
import org.opencypher.caps.api.exception.{DuplicateSourceColumnException, IllegalArgumentException, IllegalStateException}
import org.opencypher.caps.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.caps.flink.schema.EntityTable._
import org.opencypher.caps.flink.FlinkUtils._
import org.opencypher.caps.flink.schema.{EntityTable, NodeTable, RelationshipTable}
import org.opencypher.caps.impl.record._
import org.opencypher.caps.impl.util.PrintOptions
import org.opencypher.caps.ir.api.expr._
import org.opencypher.caps.ir.api.{Label, PropertyKey}

import scala.annotation.tailrec

sealed abstract class CAPFRecords(override val header: RecordHeader, val data: Table)
                                 (implicit val capf: CAPFSession) extends CypherRecords with Serializable {

  override def print(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)

  override def register(name: String): Unit =
    capf.tableEnv.registerTable(name, data)

  override def size: Long = capf.tableEnv.toDataSet[Row](data).count()

  override def columns: Set[String] = header.fields

  override def rows: Iterator[String => CypherValue] = ???

  override def iterator: Iterator[CypherMap] = ???

  override lazy val columnType: Map[String, CypherType] = data.columnType

  def unionAll(header: RecordHeader, other: CAPFRecords): CAPFRecords = {
    val unionData = data.union(other.data)
    CAPFRecords.verifyAndCreate(header, data.distinct())
  }

  def alignWith(v: Var, targetHeader: RecordHeader): CAPFRecords = {
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

    val slotDataSelectors: Seq[Row => Any] = targetHeader.slots.map { targetSlot =>
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

//    val alignedData = this.toTable().getSchema.getColumnNames.map( colName => {
//      val alignedRow = slotDataSelectors.map(_ (colName))
//    })
    CAPFRecords.empty()
  }

  def toTable(): Table = data
}

object CAPFRecords extends CypherRecordsCompanion[CAPFRecords, CAPFSession] {

  private[flink] val placeHolderVarName = ""

  def create(entityTable: EntityTable)(implicit capf: CAPFSession): CAPFRecords = {
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
        case (label, sourceColumn) => {
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
      case nt: NodeTable => sourceColumnNodeToExpressionMapping(nt.mapping)
      case rt: RelationshipTable => sourceColumnRelationshipToExpressionMapping(rt.mapping)
    }

    val (sourceHeader, sourceTable) = {
      prepareTable(entityTable.table)
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
    val renamed = sourceTable.select(newHeader.internalHeader.slots.map(ColumnName.of(_)).mkString(", "))

    CAPFRecords.createInternal(newHeader, renamed)
  }

  private def prepareTable(initialTable: Table)(implicit capf: CAPFSession): (RecordHeader, Table) = {
//    TODO: generalize datatypes
    val initialHeader = RecordHeader.from(initialTable.getSchema.getColumnNames.map { colName =>
      OpaqueField(
        Var(colName)(fromFlinkType(initialTable.getSchema.getType(colName).getOrElse(
          throw IllegalArgumentException("")
        )).getOrElse(
          throw IllegalArgumentException("")
        ))
      )
    }: _*)
    val reorderedTable = initialTable.select(initialHeader.internalHeader.slots.map(ColumnName.of(_)).mkString(", "))
    (initialHeader, reorderedTable) // TODO: rename columns
  }

  private def generalizeClumnTypes(initialTable: Table)(implicit capf: CAPFSession): Table = {
    val toCast = initialTable.getSchema.getColumnNames.filter(f => fromFlinkType(initialTable.getSchema.getType(f).getOrElse(
      throw IllegalArgumentException("")
    )).isEmpty)
//    TODO: upcasting
//    val typeCastMap = toCast.foldLeft(initialTable) {
//      case (table, col) =>
//        val castType = cypherCompatibleDataType(initialTable.getSchema.getType(col).getOrElse(
//          throw IllegalArgumentException("")
//        )).getOrElse(
//          throw IllegalArgumentException("")
//        )
//    }

    initialTable
  }

  def verifyAndCreate(headerAndData: (RecordHeader, Table))(implicit capf: CAPFSession): CAPFRecords = {
    verifyAndCreate(headerAndData._1, headerAndData._2)
  }

  def verifyAndCreate(initialHeader: RecordHeader, initialData: Table)(implicit  capf: CAPFSession): CAPFRecords = {
    // TODO: check for same session
    val initialDataColumns = initialData.columns.toSeq

    val duplicateColumns = initialDataColumns.groupBy(identity).collect {
      case (key, values) if values.size > 1 => key
    }

    if (duplicateColumns.nonEmpty)
      throw IllegalArgumentException("")

    initialHeader.slots.foreach { slot =>
      val tableSchema = initialData.getSchema
      val fieldName = ColumnName.of(slot)
      val cypherType = fromFlinkType(tableSchema.getType(fieldName).get)
      val headerType = slot.content.cypherType

      if (toFlinkType(headerType) != toFlinkType(cypherType.get) && !containsEntity(headerType))
        throw IllegalArgumentException(s"a valid data type for column ${fieldName}")
    }
    createInternal(initialHeader, initialData)
  }

  def empty(initialHeader: RecordHeader = RecordHeader.empty)(implicit capf: CAPFSession): CAPFRecords = {
    val nameToFlinkTypeMapping = initialHeader.internalHeader.slots.map(slot => ColumnName.of(slot) -> toFlinkType(slot.content.cypherType))

//    TODO: rename columns and set correct datatypes
    val emptyDataSet = capf.env.fromCollection[Row](Set.empty[Row])
    val initialTable = capf.tableEnv.fromDataSet(emptyDataSet)
    createInternal(initialHeader, initialTable)
  }

  private def createInternal(header: RecordHeader, data: Table)(implicit capf: CAPFSession) =
    new CAPFRecords(header, data) {}

  @tailrec
  private def containsEntity(t: CypherType): Boolean = t match {
    case _: CTNode => true
    case _: CTRelationship => true
    case l: CTList => containsEntity(l.elementType)
    case _ => false
  }

  override def unit()(implicit session: CAPFSession): CAPFRecords = ???
}