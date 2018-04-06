package org.opencypher.flink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions.{Expression, Literal, UnresolvedFieldReference}
import org.opencypher.flink.FlinkUtils._
import org.opencypher.flink.physical.CAPFRuntimeContext
import org.opencypher.okapi.api.types.{CTAny, CTList, CTNode, CTString}
import org.opencypher.okapi.api.value.CypherValue.CypherList
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader

object FlinkSQLExprMapper {

  implicit class RichExpression(expr: Expr) {

    def verify(implicit header: RecordHeader): Unit = {
      if (header.slotsFor(expr).isEmpty) throw IllegalStateException(s"No slot for expression $expr")
    }

    def compare(comparator: Expression => (Expression => Expression), lhs: Expr, rhs: Expr)
      (implicit header: RecordHeader, table: Table, context: CAPFRuntimeContext): Expression = {
      comparator(lhs.asFlinkSQLExpr)(rhs.asFlinkSQLExpr)
    }

    def lt(e: Expression): Expression => Expression = e < _

    def lteq(e: Expression): Expression => Expression = e <= _

    def gt(e: Expression): Expression => Expression = e > _

    def gteq(e: Expression): Expression => Expression = e >= _

    def asFlinkSQLExpr(implicit header: RecordHeader, table: Table, context: CAPFRuntimeContext): Expression = {

      expr match {

        case p@Param(name) if p.cypherType.subTypeOf(CTList(CTAny)).maybeTrue =>
          context.parameters(name) match {
            case CypherList(l) =>
              val expressionList = l.unwrap.map( elem => Literal(elem, TypeInformation.of(elem.getClass)))
              array(expressionList.head, expressionList.tail: _*)
            case notAList => throw IllegalArgumentException("a Cypher list", notAList)
          }
        case Param(name) =>
          Literal(context.parameters(name).unwrap, TypeInformation.of(context.parameters(name).unwrap.getClass))

        case _: Var | _: Param | _:Property | _: HasLabel | _: StartNode | _: EndNode =>
          verify

          val slot = header.slotsFor(expr).head

          val columns = table.getSchema.getColumnNames
          val colName = ColumnName.of(slot)

          if (columns.contains(colName)) {
            UnresolvedFieldReference(colName)
          } else {
            null
          }

        case ListLit(exprs) =>
          val flinkExpressions = exprs.map(_.asFlinkSQLExpr)
          array(flinkExpressions.head, flinkExpressions.tail: _*)

        case NullLit(_) =>
          "Null"

        case l: Lit[_] => Literal(l.v, TypeInformation.of(l.v.getClass))

        case Equals(e1, e2) => e1.asFlinkSQLExpr === e2.asFlinkSQLExpr
        case Not(e) => !e.asFlinkSQLExpr
        case IsNull(e) => e.asFlinkSQLExpr.isNull
        case IsNotNull(e) => e.asFlinkSQLExpr.isNotNull
        case Size(e) =>
          val col = e.asFlinkSQLExpr
          e.cypherType match {
            case CTString => col.charLength().cast(Types.LONG)
            case _: CTList => col.cardinality().cast(Types.LONG)
            case other => throw NotImplementedException(s"size() on values of type $other")
          }

        case Ands(exprs) =>
          exprs.map(_.asFlinkSQLExpr).foldLeft(Literal(true, Types.BOOLEAN): Expression)(_ && _)

        case Ors(exprs) =>
          exprs.map(_.asFlinkSQLExpr).foldLeft(Literal(false, Types.BOOLEAN): Expression)(_ || _)

        case In(lhs, rhs) =>
          val element = lhs.asFlinkSQLExpr
          val array = rhs.asFlinkSQLExpr
          element in array

        case As(lhs, rhs) =>
          lhs match {
            case e: Expr =>
              e.asFlinkSQLExpr as Symbol(ColumnName.of(rhs))
            case _ =>
              Symbol(ColumnName.of(lhs)) as Symbol(ColumnName.of(rhs))
          }

        case HasType(rel, relType) =>
          Type(rel)().asFlinkSQLExpr === relType.name

        case LessThan(lhs, rhs) => compare(lt, lhs, rhs)
        case LessThanOrEqual(lhs, rhs) => compare(lteq, lhs, rhs)
        case GreaterThanOrEqual(lhs, rhs) => compare(gteq, lhs, rhs)
        case GreaterThan(lhs, rhs) => compare(gt, lhs, rhs)

        case Add(lhs, rhs) => lhs.asFlinkSQLExpr + rhs.asFlinkSQLExpr
        case Subtract(lhs, rhs) => lhs.asFlinkSQLExpr - rhs.asFlinkSQLExpr
        case Multiply(lhs, rhs) => lhs.asFlinkSQLExpr * rhs.asFlinkSQLExpr
        case div@Divide(lhs, rhs) => (lhs.asFlinkSQLExpr / rhs.asFlinkSQLExpr).cast(toFlinkType(div.cypherType))

        case Exists(e) => e.asFlinkSQLExpr.isNotNull
        case Id(e) => e.asFlinkSQLExpr
        case Labels(e) =>
          val node = Var(ColumnName.of(header.slotsFor(e).head))(CTNode)
          val labelExprs = header.labels(node)
          val labelColumns = labelExprs.map(_.asFlinkSQLExpr)
          val labelNames = labelExprs.map(_.label.name)
          val booleanLabelFlagColumn = array(labelColumns.head, labelColumns.tail: _*)
          ???
//          TODO

        case Keys(e) =>
          val node = Var(ColumnName.of(header.slotsFor(e).head))(CTNode)
          val propertyExprs = header.properties(node)
          val propertyColumns = propertyExprs.map(_.asFlinkSQLExpr)
          val keyNames = propertyExprs.map(_.key.name)
          val valuesColumn = array(propertyColumns.head, propertyColumns.tail: _*)
          ???
//          TODO

        case Type(inner) =>
          inner match {
            case v: Var =>
              val typeSlot = header.typeSlot(v)
              Symbol(ColumnName.of(typeSlot))
            case _ =>
              throw NotImplementedException(s"Inner expression $inner of $expr is not yet supported (only variables)")
          }

        case StartNodeFunction(e) =>
          val rel = Var(ColumnName.of(header.slotsFor(e).head))(CTNode)
          header.sourceNodeSlot(rel).content.key.asFlinkSQLExpr

        case EndNodeFunction(e) =>
          val rel = Var(ColumnName.of(header.slotsFor(e).head))(CTNode)
          header.targetNodeSlot(rel).content.key.asFlinkSQLExpr

        case ToFloat(e) => e.asFlinkSQLExpr.cast(Types.DOUBLE)

        case ep: ExistsPatternExpr => ep.targetField.asFlinkSQLExpr

        case Coalesce(es) =>
          val columns = es.map(_.asFlinkSQLExpr)
          columns.find(_.isNotNull == true).get

        case c: CaseExpr =>
          val alternatives = c.alternatives.map {
            case (predicate, action) => // TODO
          }
          ???
      }

    }
  }

}
