package org.opencypher.flink

import org.apache.flink.table.api.TableSchema
import org.opencypher.flink.FlinkUtils._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.{InternalHeader, OpaqueField, RecordHeader, RecordSlot}

object CAPFRecordHeader {

  def fromFlinkTableSchema(schema: TableSchema): RecordHeader = {
    RecordHeader.from(schema.getColumnNames.map { field =>
      OpaqueField(
        // TODO: remove cypherCompatibleDataType call
        Var(field)(fromFlinkType(cypherCompatibleDataType(schema.getType(field).get).get)
            .getOrElse(throw IllegalArgumentException("a supported Flink type", schema.getType(field).get)))
      )
    }: _*)
  }

  implicit class CAPFInternalHeader(internalHeader: InternalHeader) {
    def columns = internalHeader.slots.map(computeColumnName).toVector

    def column(slot: RecordSlot) = columns(slot.index)

    private def computeColumnName(slot: RecordSlot): String = ColumnName.of(slot)
  }

  implicit class CAPFRecordHeader(header: RecordHeader)(implicit capf: CAPFSession) extends Serializable {
    def asFlinkTableSchema: TableSchema = ???

  }

}
