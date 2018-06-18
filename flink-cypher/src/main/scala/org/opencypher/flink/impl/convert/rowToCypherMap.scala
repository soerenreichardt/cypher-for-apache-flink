package org.opencypher.flink.impl.convert

import org.apache.flink.types.Row
import org.opencypher.flink.api.value.{CAPFNode, CAPFRelationship}
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNull, CypherValue}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.relational.impl.table.RecordHeader

final case class rowToCypherMap(exprToColumn: Seq[(Expr, String)], columnNameToIndex: Map[String, Int]) extends (Row => CypherMap) {

  private val header = RecordHeader(exprToColumn.toMap)

  override def apply(row: Row): CypherMap = {
    val values = header.vars.map(v => v.name -> constructValue(row, v)).toSeq
    CypherMap(values: _*)
  }

  private def constructValue(row: Row, v: Var): CypherValue = {
    v.cypherType match {
      case _: CTNode =>
        collectNode(row, v)

      case _: CTRelationship =>
        collectRel(row, v)

      case _ =>
        val raw = row.getField(columnNameToIndex(header.column(v)))
        CypherValue(raw)
    }
  }

  private def collectNode(row: Row, v: Var): CypherValue = {
    val idValue = row.getField(columnNameToIndex(header.column(v))).asInstanceOf[Any]
    idValue match {
      case null => CypherNull
      case id: Long =>

        val labels = header
          .labelsFor(v)
          .map { l => l.label.name -> row.getField(columnNameToIndex(header.column(l))).asInstanceOf[Boolean] }
          .collect { case (name, true) => name }

        val properties = header
          .propertiesFor(v)
          .map { p => p.key.name -> CypherValue(row.getField(columnNameToIndex(header.column(p)))) }
          .collect { case (key, value) if !value.isNull => key -> value }
          .toMap

        CAPFNode(id, labels, properties)
      case invalidID => throw UnsupportedOperationException(s"CAPFNode ID has to be a Long instead of ${invalidID.getClass}")
    }
  }

  private def collectRel(row: Row, v: Var): CypherValue = {
    val idValue = row.getField(columnNameToIndex(header.column(v))).asInstanceOf[Any]
    idValue match {
      case null => CypherNull
      case id: Long =>
        val source = row.getField(columnNameToIndex(header.column(header.startNodeFor(v)))).asInstanceOf[Long]
        val target = row.getField(columnNameToIndex(header.column(header.startNodeFor(v)))).asInstanceOf[Long]

        val relType = header
          .typesFor(v)
          .map { l => l.relType.name -> row.getField(columnNameToIndex(header.column(l))).asInstanceOf[Boolean] }
          .collect { case (name, true) => name }
          .head

        val properties = header
          .propertiesFor(v)
          .map { p => p.key.name -> CypherValue(row.getField(columnNameToIndex(header.column(p))).asInstanceOf[Any]) }
          .collect { case (key, value) if !value.isNull => key -> value }
          .toMap

        CAPFRelationship(id, source, target, relType, properties)
      case invalidID => throw UnsupportedOperationException(s"CAPFRelationship ID has to be a Long instead of ${invalidID.getClass}")
    }
  }
}