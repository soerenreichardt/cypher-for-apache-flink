package org.opencypher.flink.impl

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions._
import org.apache.flink.types.Row
import org.opencypher.flink.api.io.CAPFEntityTable
import org.opencypher.flink.api.io.FlinkCypherTable.FlinkTable
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink.impl.convert.FlinkConversions._
import org.opencypher.flink.impl.convert.rowToCypherMap
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.table.RecordsPrinter
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.io.RelationalCypherRecords
import org.opencypher.okapi.relational.impl.table._

import scala.annotation.tailrec

object CAPFRecords {

  def empty(initialHeader: RecordHeader = RecordHeader.empty)(implicit capf: CAPFSession): CAPFRecords = {
    val initialTableSchema = initialHeader.toResolvedFieldReference

    implicit val rowTypeInfo = new RowTypeInfo(initialTableSchema.map(_.resultType).toArray, initialTableSchema.map(_.name).toArray)
    val initialTable = capf.tableEnv.fromDataSet(
      capf.env.fromCollection(List.empty[Row]),
      initialTableSchema.map(field => UnresolvedFieldReference(field.name)): _*
    )
    CAPFRecords(initialHeader, initialTable)
  }

  def unit()(implicit capf: CAPFSession): CAPFRecords = {
    val initialTable = capf.tableEnv.fromDataSet(capf.env.fromCollection(Seq(EmptyRow())))
    CAPFRecords(RecordHeader.empty, initialTable)
  }

  def create(entityTable: CAPFEntityTable)(implicit capf: CAPFSession): CAPFRecords = {
    val withCypherCompatibleTypes = entityTable.relationalTable.table.withCypherCompatibleTypes
    CAPFRecords(entityTable.header, withCypherCompatibleTypes)
  }

  private[flink] def wrap(table: Table)(implicit capf: CAPFSession): CAPFRecords = {
    val compatibleTable = table.withCypherCompatibleTypes
    CAPFRecords(compatibleTable.getSchema.toRecordHeader, compatibleTable)
  }

  private case class EmptyRow()
}

case class CAPFRecords(header: RecordHeader, table: Table, override val logicalColumns: Option[Seq[String]]= None)
  (implicit val capf: CAPFSession)
  extends RelationalCypherRecords[FlinkTable]
  with RecordBehaviour
  with Serializable {

  override type R = CAPFRecords

  verify()

  override def from(header: RecordHeader, table: FlinkTable, displayNames: Option[Seq[String]]): CAPFRecords =
    copy(header, table.table, displayNames)

  override def relationalTable: FlinkTable = table

  def toTable(colNames: String*): Table = colNames match {
    case Nil => table
    case _ => relationalTable.table.select(colNames.map(UnresolvedFieldReference): _*)
  }

  def cache(): CAPFRecords = ???

  def retag(replacements: Map[Int, Int]): CAPFRecords = {
    val actualRetaggings = replacements.filterNot { case (from, to) => from == to }
    val idColumns = header.idColumns()
    val tableWithReplacedTags = idColumns.foldLeft(table) {
      case (t, column) => t.safeReplaceTags(column, actualRetaggings)
    }
    copy(header, tableWithReplacedTags)
  }

  def retagVariable(v: Var, replacements: Map[Int, Int]): CAPFRecords = {
    val columnsToUpdate = header.idColumns(v)
    val updatedData = columnsToUpdate.foldLeft(table) { case (table, columnName) =>
      table.safeReplaceTags(columnName, replacements)
    }
    copy(header)
  }

  protected val TRUE_LIT: Expression = Literal(true, Types.BOOLEAN)
  protected val FALSE_LIT: Expression = Literal(false, Types.BOOLEAN)
  protected val NULL_LIT: Expression = Null(Types.BOOLEAN)

  def alignWith(v: Var, targetHeader: RecordHeader): CAPFRecords = {

    val entityVars = header.entityVars

    val oldEntity = entityVars.toSeq match {
      case Seq(one) => one
      case Nil => throw IllegalArgumentException("one entity in the record header", s"no entity in $header")
      case _ => throw IllegalArgumentException("one entity in the record header", s"multiple entities in $header")
    }

    assert(header.ownedBy(oldEntity) == header.expressions, s"header describes more than one entity")

    val entityLabels: Set[String] = oldEntity.cypherType match {
      case CTNode(labels, _) => labels
      case CTRelationship(types, _) => types
      case _ => throw IllegalArgumentException("CTNode or CTRelationship", oldEntity.cypherType)
    }

    val updatedHeader = header
      .withAlias(oldEntity as v)
      .select(v)

    val missingExpressions = targetHeader.expressions -- updatedHeader.expressions
    val overlapExpressions = targetHeader.expressions -- missingExpressions

    val dataWithColumnsRenamed = overlapExpressions.foldLeft(table) {
      case (currentTable, expr) =>
        val oldColumn = updatedHeader.column(expr)
        val newColumn = targetHeader.column(expr)
        if (oldColumn != newColumn) {
          currentTable
            .safeReplaceColumn(oldColumn, UnresolvedFieldReference(oldColumn).cast(expr.cypherType.toFlinkType.get) as Symbol(oldColumn))
            .safeRenameColumn(oldColumn, newColumn)
        } else {
          currentTable
        }
    }

    val dataWithMissingColumns = missingExpressions.foldLeft(dataWithColumnsRenamed) {
      case (currentTable, expr) =>
        val newColumn = expr match {
          case HasLabel(_, label) => if (entityLabels.contains(label.name)) TRUE_LIT else FALSE_LIT
          case HasType(_, relType) => if (entityLabels.contains(relType.name)) TRUE_LIT else FALSE_LIT
          case _ =>
            if (!expr.cypherType.isNullable) {
              throw UnsupportedOperationException(
                s"Cannot align scan on $v by adding a NULL column, because the type for '$expr' is non-nullable"
              )
            }
            NULL_LIT.cast(expr.cypherType.getFlinkType)
        }

        currentTable.safeUpsertColumn(
          targetHeader.column(expr),
          newColumn.as(Symbol(targetHeader.column(expr)))
        )
    }
    copy(targetHeader, dataWithMissingColumns)
  }

  private def verify(): Unit = {

    @tailrec
    def containsEntity(t: CypherType): Boolean = t match {
      case _: CTNode => true
      case _: CTRelationship => true
      case l: CTList => containsEntity(l.elementType)
      case _ => false
    }

    val initialDataColumns = relationalTable.physicalColumns.toSeq

    val duplicateColumns = initialDataColumns.groupBy(identity).collect {
      case (key, values) if values.size > 1 => key
    }

    if (duplicateColumns.nonEmpty)
      throw IllegalArgumentException(
        "a DataFrame with distinct columns",
        s"a DataFrame with duplicate columns: ${initialDataColumns.sorted.mkString("[", ", ", "]")}"
      )

    val headerColumnNames = header.columns
    val dataColumnNames = relationalTable.physicalColumns.toSet
    val missingColumnNames = headerColumnNames -- dataColumnNames
    if (missingColumnNames.nonEmpty) {
      throw IllegalArgumentException(
        s"data with columns ${header.columns.toSeq.sorted.mkString("\n[", ", ", "]\n")}",
        s"data with columns ${dataColumnNames.toSeq.sorted.mkString("\n[", ", ", "]\n")}"
      )
    }

    header.expressions.foreach { expr =>
      val tableSchema = table.getSchema
      val field = header.column(expr)
      val cypherType = tableSchema.getType(field).get.toCypherType().getOrElse(
        throw IllegalArgumentException("a supported Flink type", field)
      )
      val headerType = expr.cypherType

      if (headerType.toFlinkType != cypherType.toFlinkType && !containsEntity(headerType))
        throw IllegalArgumentException(s"a valid data type for column $field of type $headerType", cypherType)
    }
  }

  override def toString: String = {
    val numRows = relationalTable.size
    if (header.isEmpty && numRows == 0) {
      s"CAPFRecords.empty"
    } else if (header.isEmpty && numRows == 1) {
      s"CAPFRecords.unit"
    } else {
      s"CAPFRecords($header, table with $numRows rows)"
    }
  }

}

trait RecordBehaviour extends RelationalCypherRecords[FlinkTable] {

  override def show(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)

  override lazy val columnType: Map[String, CypherType] = relationalTable.table.columnType

  override def rows: Iterator[String => CypherValue] = relationalTable.table.rows

  override def iterator: Iterator[CypherMap] = toCypherMaps.collect().iterator

  def toLocalIterator: Iterator[CypherMap] = iterator

  override def collect: Array[CypherMap] = toCypherMaps.collect().toArray

  override def size: Long = relationalTable.table.count()

  def toCypherMaps: DataSet[CypherMap] = {
    relationalTable.table.toDataSet[Row].map(rowToCypherMap(header.exprToColumn.toSeq, relationalTable.table.getSchema.columnNameToIndex))
  }
}
