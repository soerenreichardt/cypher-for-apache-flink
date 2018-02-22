package org.opencypher.caps.flink

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.caps.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.caps.flink.CAPFRecordHeaderConversions.CAPFRecordHeader
import org.opencypher.caps.flink.FlinkUtils._
import org.opencypher.caps.flink.schema.EntityTable._
import org.opencypher.caps.impl.exception.{IllegalArgumentException, IllegalStateException}
import org.opencypher.caps.impl.table.{CypherRecordsCompanion, RecordHeader, RecordsPrinter}
import org.opencypher.caps.impl.util.PrintOptions
import org.opencypher.caps.ir.api.Label
import org.opencypher.caps.ir.api.expr.{HasLabel, Type, Var}

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