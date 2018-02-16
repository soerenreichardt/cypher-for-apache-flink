package org.opencypher.caps.flink

import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.api.types.CypherType
import org.opencypher.caps.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.caps.impl.table.{CypherRecordsCompanion, RecordHeader, RecordsPrinter}
import org.opencypher.caps.impl.util.PrintOptions
import org.opencypher.caps.flink.FlinkUtils._

sealed abstract class CAPFRecords(val header: RecordHeader, val data: Table)(implicit capf: CAPFSession)
  extends CypherRecords with Serializable {

  override def print(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)

  override def register(name: String): Unit =
    capf.tableEnv.registerTable(name, data)

  override def size: Long = capf.tableEnv.toDataSet[Row](data).count()

  override def columns: Seq[String] = header.fieldsInOrder

  override def rows: Iterator[String => CypherValue] = ???

  override def iterator: Iterator[CypherMap] = ???

  override lazy val columnType: Map[String, CypherType] = ???

}

object CAPFRecords extends CypherRecordsCompanion[CAPFRecords, CAPFSession] {

  def empty(initialHeader: RecordHeader = RecordHeader.empty)(implicit capf: CAPFSession): CAPFRecords = {
    val nameToFlinkTypeMapping = initialHeader.internalHeader.slots.map(slot => ColumnName.of(slot) -> toFlinkType(slot.content.cypherType))

    //    TODO: rename columns and set correct datatypes
    val emptyDataSet = capf.env.fromCollection[Row](Set.empty[Row])
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