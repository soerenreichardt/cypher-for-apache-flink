package org.opencypher.flink

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{Expression, Literal, Null, UnresolvedFieldReference}
import org.apache.flink.types.Row
import org.opencypher.flink.CAPFRecordHeader._
import org.opencypher.flink.FlinkUtils._
import org.opencypher.flink.TableOps._
import org.opencypher.flink.CAPFCypherType._
import org.opencypher.flink.schema.{CAPFEntityTable, CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.schema.EntityTable._
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.table.{CypherRecords, CypherRecordsCompanion}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, UnsupportedOperationException}
import org.opencypher.okapi.impl.table.RecordsPrinter
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.relational.impl.exception.DuplicateSourceColumnException
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.okapi.relational.impl.syntax.RecordHeaderSyntax._

sealed abstract class CAPFRecords(val header: RecordHeader, val data: Table)(implicit val capf: CAPFSession)
  extends CypherRecords with Serializable {

  override def show(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)

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

  def retag(replacements: Map[Int, Int]): CAPFRecords = {
    val actualRetaggings = replacements.filterNot { case (from, to) => from == to }
    val idColumns = header.contents.collect {
      case f: OpaqueField => ColumnName.of(f)
      case p@ProjectedExpr(StartNode(_)) => ColumnName.of(p)
      case p@ProjectedExpr(EndNode(_)) => ColumnName.of(p)
    }
    val tableWithReplacedTags = idColumns.foldLeft(data) {
      case (table, column) => table.safeReplaceTags(column, actualRetaggings)
    }

    CAPFRecords.verifyAndCreate(header, tableWithReplacedTags)
  }

  def select(fields: Set[Var]): CAPFRecords = {
    val selectedHeader = header.select(fields)
    val selectedColumnNames = selectedHeader.contents.map(ColumnName.of).toSeq
    val selectedColumns = data.select(selectedColumnNames.map(UnresolvedFieldReference): _*)
    CAPFRecords.verifyAndCreate(selectedHeader, selectedColumns)
  }

  def compact(implicit details: RetainedDetails): CAPFRecords = {
    val cachedHeader = header.update(compactFields)._1
    if (header == cachedHeader) {
      this
    } else {
      val cachedData = {
        val columns = cachedHeader.slots.map(c => UnresolvedFieldReference(ColumnName.of(c.content)))
        data.select(columns: _*)
      }

      CAPFRecords.verifyAndCreate(cachedHeader, cachedData)
    }
  }

  def addAliases(aliasToOriginal: Map[Var, Var]): CAPFRecords = {
    val (updatedHeader, updatedData) = aliasToOriginal.foldLeft((header, data)) {
      case ((tempHeader, tempTable), (nextAlias, nextOriginal)) =>
        val originalSlots = tempHeader.selfWithChildren(nextOriginal).toList
        val slotsToAdd = originalSlots.map(_.withOwner(nextAlias))
        val updatedHeader = tempHeader ++ RecordHeader.from(slotsToAdd)
        val originalColumns = originalSlots.map(ColumnName.of).map(UnresolvedFieldReference)
        val aliasColumns = slotsToAdd.map(ColumnName.of)
        val additions = aliasColumns.zip(originalColumns)
        val updatedTable = tempTable.safeAddColumns(additions: _*)
        updatedHeader -> updatedTable
    }
    CAPFRecords.verifyAndCreate(updatedHeader, updatedData)
  }

  def retagVariable(v: Var, replacements: Map[Int, Int]): CAPFRecords = {
    val slotsToRetag = v.cypherType match {
      case _: CTNode => Set(header.slotFor(v))
      case _: CTRelationship =>
        val idSlot = header.slotFor(v)
        val sourceSlot = header.sourceNodeSlot(v)
        val targetSlot = header.targetNodeSlot(v)
        Set(idSlot, sourceSlot, targetSlot)
      case _ => Set.empty
    }
    val columnsToRetag = slotsToRetag.map(ColumnName.of)
    val retaggedData = columnsToRetag.foldLeft(data) { case (table, columnName) =>
    table.safeReplaceTags(columnName, replacements)
    }
    CAPFRecords.verifyAndCreate(header, retaggedData)
  }

  def renameVars(aliasToOriginal: Map[Var, Var]): CAPFRecords = {
    val (updatedHeader, updatedData) = aliasToOriginal.foldLeft((header, data)) {
      case ((tempHeader, tempTable), (nextAlias, nextOriginal)) =>
        val slotsToReassign = tempHeader.selfWithChildren(nextOriginal).toList
        val reassignedSlots = slotsToReassign.map(_.withOwner(nextAlias))
        val updatedHeader = tempHeader --
          RecordHeader.from(slotsToReassign) ++
          RecordHeader.from(reassignedSlots)
        val originalColumns = slotsToReassign.map(ColumnName.of)
        val aliasColumns = reassignedSlots.map(ColumnName.of)
        val updatedTable = tempTable.safeRenameColumns(originalColumns, aliasColumns)
        updatedHeader -> updatedTable
    }
    CAPFRecords.verifyAndCreate(updatedHeader, updatedData)
  }

  def removeVars(vars: Set[Var]): CAPFRecords = {
    val (updatedHeader, updatedData) = vars.foldLeft((header, data)) {
      case ((tempHeader, tempTable), nextFieldToRemove) =>
        val slotsToRemove = tempHeader.selfWithChildren(nextFieldToRemove)
        val updatedHeader = tempHeader -- RecordHeader.from(slotsToRemove.toList)
        updatedHeader -> tempTable.safeDropColumns(slotsToRemove.map(ColumnName.of): _*)
    }
    CAPFRecords.verifyAndCreate(updatedHeader, updatedData)
  }

  def unionAll(header: RecordHeader, other: CAPFRecords): CAPFRecords = {
    val unionData = data.union(other.data)
    CAPFRecords.verifyAndCreate(header, unionData)
  }

  def distinct: CAPFRecords = {
    CAPFRecords.verifyAndCreate(header, data.distinct())
  }

  override def collect: Array[CypherMap] =
    toCypherMaps.collect().toArray

  def alignWith(v: Var, targetHeader: RecordHeader): CAPFRecords = {
    val oldEntity = this.header.fieldsAsVar.headOption
      .getOrElse(throw IllegalStateException("GraphScan table did not contain any fields"))

    val entityLabels: Set[String] = oldEntity.cypherType match {
      case CTNode(labels, _) => labels
      case CTRelationship(typ, _) => typ
      case _ => throw IllegalArgumentException("CTNode or CTRelationship", oldEntity.cypherType)
    }

    val renamedSlotMaping = this.header.slots.map { slot =>
      val withNewOwner = slot.withOwner(v).content
      slot -> targetHeader.slots.find(slot => slot.content == withNewOwner).get
    }

    val withRenamedColumns = renamedSlotMaping.foldLeft(data) {
      case (acc, (oldCol, newCol)) =>
        val oldColName = ColumnName.of(oldCol)
        val newColName = ColumnName.of(newCol)
        if (oldColName != newColName) {
          acc
            .safeReplaceColumn(oldColName, UnresolvedFieldReference(oldColName).cast(newCol.content.cypherType.toFlinkType.get))
            .safeRenameColumn(oldColName, newColName)
        } else {
          acc
        }
    }

    val renamedSlots = renamedSlotMaping.map(_._2)

    def typeAlignmentError(alignedWithSlot: RecordSlot, varToAlign: Var, typeToAlign: CypherType) = {
      val varToAlignName = varToAlign.name
      val varToAlignString = if (varToAlignName.isEmpty) "table" else s"variable '$varToAlignName'"

      throw UnsupportedOperationException(
        s"""
           |Cannot align $varToAlignString with '${v.name}' due the alignment target type for ${alignedWithSlot.content.key.withoutType}:
           |  The target type on '${v.name}' is ${alignedWithSlot.content.cypherType}, whilst the $varToAlignString type is $typeToAlign
         """.stripMargin
      )
    }

    val relevantColumns = targetHeader.slots.map { targetSlot =>
      val targetColName = ColumnName.of(targetSlot)

      renamedSlots.find(_.content == targetSlot.content) match {
        case Some(sourceSlot) =>
          val sourceColName = ColumnName.of(sourceSlot)

          // the column exists in the source data
          if (sourceColName == targetColName) {
            UnresolvedFieldReference(targetColName)
          } else {
            val slotType = targetSlot.content.cypherType
            val flinkTypeOpt = slotType.toFlinkType
            flinkTypeOpt match {
              case Some(flinkType) =>
                sourceColName.cast(flinkType) as Symbol(targetColName)
              case None => typeAlignmentError(targetSlot, oldEntity, sourceSlot.content.cypherType)
            }
          }

        case None =>
          val content = targetSlot.content.key match {
            case HasLabel(_, label) if entityLabels.contains(label.name) => Literal(true, Types.BOOLEAN)
            case _: HasLabel => Literal(false, Types.BOOLEAN)
            case _: Type if entityLabels.size == 1 => Literal(entityLabels.head, Types.STRING)
            case _ => Null(targetSlot.content.cypherType.getFlinkType)
          }
          content as Symbol(targetColName)
      }
    }

    val withRelevantColumns = withRenamedColumns.select(relevantColumns: _*)
    CAPFRecords.verifyAndCreate(targetHeader, withRelevantColumns)
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
      prepareTable(entityTable.table.table)
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
    val expressions = newHeader.slots.map(ColumnName.of).map(UnresolvedFieldReference)
    val renamed = sourceTable.as(expressions:_*)

    CAPFRecords.createInternal(newHeader, renamed)
  }

  private [flink] def wrap(table: Table)(implicit capf: CAPFSession): CAPFRecords =
    verifyAndCreate(prepareTable(table))

  private def prepareTable(initialTable: Table)(implicit capf: CAPFSession): (RecordHeader, Table) = {
    val withCompatibleTypes = generalizeColumnTypes(initialTable)
    val initialHeader = CAPFRecordHeader.fromFlinkTableSchema(withCompatibleTypes.getSchema)
    val withRenamedColumns = initialTable.as(initialHeader.slots.map(ColumnName.of).mkString(","))
    (initialHeader, withRenamedColumns)
  }

  private def generalizeColumnTypes(initialTable: Table): Table = {
    val castExprs = initialTable.getSchema.getColumnNames.zip(initialTable.getSchema.getTypes).map {
      case (fieldName, fieldType) =>
        Seq(
          UnresolvedFieldReference(fieldName).cast(fieldType.cypherCompatibleDataType.getOrElse(
            throw IllegalArgumentException(
              s"a Flink type supported by Cypher: ${supportedTypes.mkString("[", ", ", "]")}",
              s"type $fieldType of field $fieldName"
            )
          )) as Symbol(fieldName))
    }.reduce(_ ++ _)

    initialTable.select(castExprs: _*)
  }

  def empty(initialHeader: RecordHeader = RecordHeader.empty)(implicit capf: CAPFSession): CAPFRecords = {
    val nameToFlinkTypeMapping = initialHeader.slots.map(slot => UnresolvedFieldReference(ColumnName.of(slot)) -> toFlinkType(slot.content.cypherType))

    implicit val rowTypeInfo = new RowTypeInfo(nameToFlinkTypeMapping.map(_._2): _*)

    val emptyDataSet = capf.env.fromCollection(Seq.empty[Row])
    val initialTable = capf.tableEnv.fromDataSet(emptyDataSet, nameToFlinkTypeMapping.map(_._1): _*)
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
    val initialDataColumns = initialData.columns

    val duplicateColumns = initialDataColumns.groupBy(identity).collect {
      case (key, values) if values.size > 1 => key
    }

    if (duplicateColumns.nonEmpty)
      throw IllegalArgumentException(
        "a Table with destinct columns",
        s"a Table with duplicate columns: $duplicateColumns")

    val headerColumnNames = initialHeader.slots.map(ColumnName.of).toSet
    val dataColumnNames = initialData.columns.toSet
    val missingColumnNames = headerColumnNames -- dataColumnNames
    if (missingColumnNames.nonEmpty) {
      throw IllegalArgumentException(
        s"data with columns ${initialHeader.slots.map(ColumnName.of).sorted.mkString("\n", ", ", "\n")}",
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