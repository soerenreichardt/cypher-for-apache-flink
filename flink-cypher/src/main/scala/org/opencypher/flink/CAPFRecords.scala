package org.opencypher.flink

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{Expression, Null, UnresolvedFieldReference}
import org.apache.flink.types.Row
import org.opencypher.flink.CAPFRecordHeader._
import org.opencypher.flink.FlinkUtils._
import org.opencypher.flink.TableOps._
import org.opencypher.flink.schema.{CAPFEntityTable, CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.schema.EntityTable._
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.table.{CypherRecords, CypherRecordsCompanion}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException}
import org.opencypher.okapi.impl.table.RecordsPrinter
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.relational.impl.exception.DuplicateSourceColumnException
import org.opencypher.okapi.relational.impl.table._

sealed abstract class CAPFRecords(val header: RecordHeader, val data: Table)(implicit val capf: CAPFSession)
  extends CypherRecords with Serializable {

  override def show(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)

  override def register(name: String): Unit =
    capf.tableEnv.registerTable(name, data)

  override def size: Long = data.count()

  def toCypherMaps: DataSet[CypherMap] = {
    print(capf.tableEnv.explain(data))
    val dataSet = data.toDataSet[Row] // TODO: safe to dataset
    dataSet.map(rowToCypherMap(header, data.getSchema.columnNameToIndex))
  }

  override def columns: Seq[String] =
    header.fieldsInOrder

  override def rows: Iterator[String => CypherValue] =
    data.rows

  override def iterator: Iterator[CypherMap] =
    toCypherMaps.collect().iterator

  def toLocalIterator: Iterator[CypherMap] =
//  TODO: may be not a local iterator
    iterator

  override lazy val columnType: Map[String, CypherType] = data.columnType

  def cache(): CAPFRecords = {
    // TODO: cache
    this
  }

  override def collect: Array[CypherMap] =
    toCypherMaps.collect().toArray

  def alignWith(v: Var, targetHeader: RecordHeader): CAPFRecords = {
    val oldEntity = this.header.internalHeader.fields.headOption
        .getOrElse(throw IllegalStateException("GraphScan table did not contain any fields"))

    val entityLabels: Set[String] = oldEntity.cypherType match {
      case CTNode(labels) => labels
      case CTRelationship(typ) => typ
      case _ => throw IllegalArgumentException("CTNode or CTRelationship", oldEntity.cypherType)
    }

    val slots = this.header.slots
    val renamedSlotMapping = slots.map { slot =>
      slot -> slot.withOwner(v)
    }

    val renames = renamedSlotMapping.map { mapping =>
      ColumnName.of(mapping._1) -> ColumnName.of(mapping._2)
    }.map { mapping =>
      Symbol(mapping._1) as Symbol(mapping._2)
    }

    val renamedTable = this.data.select(renames:_ *)

    val renamedSlots = renamedSlotMapping.map(_._2)

    val withMissingColumns: Seq[Expression] = targetHeader.slots.map { targetSlot =>
      val targetColName = ColumnName.of(targetSlot)

      renamedSlots.find(_.content.key.withoutType == targetSlot.content.key.withoutType) match {
        case Some(sourceSlot) =>
          val sourceColName = ColumnName.of(sourceSlot)

          // the column exists in the source data
//          if (sourceColName == targetColName) {
//            sourceColName as Symbol(targetColName)
//          }
          Symbol(ColumnName.of(targetSlot)) as Symbol(ColumnName.of(targetSlot)) // TODO: check for name equality
        case None => targetSlot.content.key match {
          case HasLabel(_, l: Label) => entityLabels(l.name) as Symbol(ColumnName.of(targetSlot))
          case _: Type if entityLabels.size == 1 => entityLabels.head as Symbol(ColumnName.of(targetSlot))
          case _ => Null(toFlinkType(targetSlot.content.cypherType)) as Symbol(ColumnName.of(targetSlot))
        }
      }
    }

    val renamedTableWithMissingColumns = renamedTable.select(withMissingColumns: _*)
    CAPFRecords.verifyAndCreate(targetHeader, renamedTableWithMissingColumns)
  }

  def toTable(): Table = data

  def unionAll(header: RecordHeader, other: CAPFRecords): CAPFRecords = {
    val unionData = data.union(other.data)
    CAPFRecords.verifyAndCreate(header, unionData)
  }
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
    val expressions: Vector[Expression] = newHeader.internalHeader.columns.map(f => UnresolvedFieldReference(f))
    val renamed = sourceTable.as(expressions:_*)

    CAPFRecords.createInternal(newHeader, renamed)
  }

  private [flink] def wrap(table: Table)(implicit capf: CAPFSession): CAPFRecords =
    verifyAndCreate(prepareDataFrame(table))

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

  def verifyAndCreate(headerAndData: (RecordHeader, Table))(implicit capf: CAPFSession): CAPFRecords = {
    verifyAndCreate(headerAndData._1, headerAndData._2)
  }

  def verifyAndCreate(initialHeader: RecordHeader, initialData: Table)(implicit capf: CAPFSession): CAPFRecords = {
    val initialDataColumns = initialData.columns.toSeq

    val duplicateColumns = initialDataColumns.groupBy(identity).collect {
      case (key, values) if values.size > 1 => key
    }

    if (duplicateColumns.nonEmpty)
      throw IllegalArgumentException(
        "a Table with destinct columns",
        s"a Table with duplicate columns: $duplicateColumns")

    val headerColumnNames = initialHeader.internalHeader.columns.toSet
    val dataColumnNames = initialData.columns.toSet
    val missingColumnNames = headerColumnNames -- dataColumnNames
    if (missingColumnNames.nonEmpty) {
      throw IllegalArgumentException(
        s"data with columns ${initialHeader.internalHeader.columns.sorted.mkString("\n", ", ", "\n")}",
        s"data with missing columns ${missingColumnNames.toSeq.sorted.mkString("\n", ", ", "\n")}"
      )
    }

    initialHeader.slots.foreach { slot =>
      val tableSchema = initialData.getSchema
      val fieldName = ColumnName.of(slot)
      val fieldType = tableSchema.getType(fieldName).get
      val cypherType = fromFlinkType(fieldType)
        .getOrElse(throw IllegalArgumentException("a supported Flink type", fieldType))
      val headerType = slot.content.cypherType

      if (toFlinkType(headerType) != toFlinkType(cypherType) && !containsEntity(headerType))
        throw IllegalArgumentException(s"a valid data type for column ${fieldName} of type $headerType", cypherType)
    }
    createInternal(initialHeader, initialData)
  }

  private def createInternal(header: RecordHeader, data: Table)(implicit capf: CAPFSession) =
    new CAPFRecords(header, data) {}

  private def containsEntity(t: CypherType): Boolean = t match {
    case _: CTNode => true
    case _: CTRelationship => true
    case l: CTList => containsEntity(l.elementType)
    case _ => false
  }

  private case class EmptyRow()
}