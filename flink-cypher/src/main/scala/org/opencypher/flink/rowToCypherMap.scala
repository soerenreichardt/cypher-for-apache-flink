package org.opencypher.flink

import org.apache.flink.types.Row
import org.opencypher.flink.value.{CAPFNode, CAPFRelationship}
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNull, CypherValue}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader

final case class rowToCypherMap(header: RecordHeader, columnNameToIndex: Map[String, Int]) extends (Row => CypherMap) {

  override def apply(row: Row): CypherMap = {
    val values = header.fieldsAsVar.map { field =>
      field.name -> constructValue(row, field)
    }.toSeq

    CypherMap(values: _*)
  }

  private def constructValue(row: Row, field: Var): CypherValue = {
    field.cypherType match {
      case _: CTNode =>
        collectNode(row, field)

      case _: CTRelationship =>
        collectRel(row, field)

      case _ =>
        val raw = row.getField(columnNameToIndex(ColumnName.of(header.slotFor(field))))
        CypherValue(raw)
    }
  }

  private def collectNode(row: Row, field: Var): CypherValue = {
    val idValue = row.getField(columnNameToIndex(ColumnName.of(header.slotFor(field)))).asInstanceOf[Any]
    idValue match {
      case null         => CypherNull
      case id: Long     =>
        val labels = header
          .labelSlots(field)
          .mapValues { s =>
            row.getField(columnNameToIndex(ColumnName.of(s))).asInstanceOf[Boolean]
          }
          .collect {
            case (h, b) if b =>
              h.label.name
          }
          .toSet

        val properties = header
          .propertySlots(field)
          .mapValues { s =>
            CypherValue(row.getField(columnNameToIndex(ColumnName.of(s))))
          }
          .collect {
            case (p, v) if !v.isNull =>
              p.key.name -> v
          }

        CAPFNode(id, labels, properties)
      case invalidID => throw UnsupportedOperationException(s"CAPFNode ID has to be a Long instead of ${invalidID.getClass}")
    }
  }

  private def collectRel(row: Row, field: Var): CypherValue = {
    val idValue = row.getField(columnNameToIndex(ColumnName.of(header.slotFor(field)))).asInstanceOf[Any]
    idValue match {
      case null         => CypherNull
      case id: Long     =>
        val source = row.getField(columnNameToIndex(ColumnName.of(header.sourceNodeSlot(field)))).asInstanceOf[Long]
        val target = row.getField(columnNameToIndex(ColumnName.of(header.targetNodeSlot(field)))).asInstanceOf[Long]
        val typ = row.getField(columnNameToIndex(ColumnName.of(header.typeSlot(field)))).asInstanceOf[String]
        val properties = header
          .propertySlots(field)
          .mapValues { s =>
            CypherValue.apply(row.getField(columnNameToIndex(ColumnName.of(s))).asInstanceOf[Any])
          }
          .collect {
            case (p, v) if !v.isNull =>
              p.key.name -> v
          }

        CAPFRelationship(id, source, target, typ, properties)
      case invalidID => throw UnsupportedOperationException(s"CAPFRelationship ID has to be a Long instead of ${invalidID.getClass}")
    }
  }

}
