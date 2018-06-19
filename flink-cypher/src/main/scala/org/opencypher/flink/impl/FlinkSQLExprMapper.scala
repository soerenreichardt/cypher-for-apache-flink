package org.opencypher.flink.impl

import org.apache.commons.cli.MissingArgumentException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink.impl.convert.FlinkConversions._
import org.opencypher.flink.impl.physical.CAPFRuntimeContext
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherList
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader

object FlinkSQLExprMapper {

  implicit class RichExpression(expr: Expr) {

    def verify(implicit header: RecordHeader): Unit = {
      if (header.expressionsFor(expr).isEmpty) throw IllegalStateException(s"No slot for expression $expr")
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
              val expressionList = l.unwrap.map( elem => expressions.Literal(elem, TypeInformation.of(elem.getClass)))
              if (expressionList.isEmpty)
                throw new NotImplementedException("Empty lists are not yet supported by Flink")

              array(expressionList.head, expressionList.tail: _*)
            case notAList => throw IllegalArgumentException("a Cypher list", notAList)
          }
        case Param(name) =>
          expressions.Literal(context.parameters(name).unwrap, TypeInformation.of(context.parameters(name).unwrap.getClass))

        case _: Var | _: Param | _:Property | _: HasLabel | _: HasType | _: StartNode | _: EndNode =>
          verify

          val colName = header.column(expr)
          if (table.columns.contains(colName)) {
            expressions.UnresolvedFieldReference(colName)
          } else {
            expressions.Null(expr.cypherType.getFlinkType) as Symbol(colName)
          }

        case ListLit(exprs) =>
          val flinkExpressions = exprs.map(_.asFlinkSQLExpr)
          array(flinkExpressions.head, flinkExpressions.tail: _*)

        case n: NullLit =>
          val tpe = n.cypherType.getFlinkType
          expressions.Null(tpe)

        case l: Lit[_] => expressions.Literal(l.v, TypeInformation.of(l.v.getClass))

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
          exprs.map(_.asFlinkSQLExpr).foldLeft(expressions.Literal(true, Types.BOOLEAN): Expression)(_ && _)

        case Ors(exprs) =>
          exprs.map(_.asFlinkSQLExpr).foldLeft(expressions.Literal(false, Types.BOOLEAN): Expression)(_ || _)

        case In(lhs, rhs) =>
          val element = lhs.asFlinkSQLExpr
          val array = rhs.asFlinkSQLExpr
          element in array

        case As(lhs, rhs) =>
          val colName = header.column(rhs)
          lhs match {
            case e: Lit[_] =>
              e.asFlinkSQLExpr as Symbol(colName)
            case _ =>
              Symbol(header.column(lhs)) as Symbol(colName)
          }

        case LessThan(lhs, rhs) => compare(lt, lhs, rhs)
        case LessThanOrEqual(lhs, rhs) => compare(lteq, lhs, rhs)
        case GreaterThanOrEqual(lhs, rhs) => compare(gteq, lhs, rhs)
        case GreaterThan(lhs, rhs) => compare(gt, lhs, rhs)

        case Add(lhs, rhs) => lhs.asFlinkSQLExpr + rhs.asFlinkSQLExpr
        case Subtract(lhs, rhs) => lhs.asFlinkSQLExpr - rhs.asFlinkSQLExpr
        case Multiply(lhs, rhs) => lhs.asFlinkSQLExpr * rhs.asFlinkSQLExpr
        case div@Divide(lhs, rhs) => (lhs.asFlinkSQLExpr / rhs.asFlinkSQLExpr).cast(div.cypherType.getFlinkType)

        case Exists(e) => e.asFlinkSQLExpr.isNotNull
        case Id(e) => e.asFlinkSQLExpr
//        case Labels(e)=>
//          val node = Var(ColumnName.of(header.slotsFor(e).head))(CTNode)
//          val labelExprs = header.labels(node)
//          val labelColumns = labelExprs.map(_.asFlinkSQLExpr)
//          val labelNames = labelExprs.map(_.label.name)
//          val booleanLabelFlagColumn = array(labelColumns.head, labelColumns.tail: _*)
//          ???
//          TODO

        case Keys(e) =>
          val node = e.owner.get
          val propertyExprs = header.propertiesFor(node).toSeq.sortBy(_.key.name)
          val (propertyKeys, propertyColumns) = propertyExprs.map(e => e.key.name -> e.asFlinkSQLExpr).unzip
          val valuesColumn = array(propertyColumns.head, propertyColumns.tail: _*)
          ???
//          TODO

        case Type(inner) =>
          inner match {
            case v: Var =>
              val typeExprs = header.typesFor(v)
              val (relTypeNames, relTypeColumns) = typeExprs.toSeq.map(e => e.relType.name -> e.asFlinkSQLExpr).unzip

              val getTypesFunction = new GetTypes(relTypeNames: _*)
              getTypesFunction(relTypeColumns: _*)
            case _ =>
              throw NotImplementedException(s"Inner expression $inner of $expr is not yet supported (only variables)")
          }

        case StartNodeFunction(e) =>
          val rel = e.owner.get
          header.startNodeFor(rel).asFlinkSQLExpr

        case EndNodeFunction(e) =>
          val rel = e.owner.get
          header.endNodeFor(rel).asFlinkSQLExpr

        case ToFloat(e) => e.asFlinkSQLExpr.cast(Types.DOUBLE)


        case ep: ExistsPatternExpr => ep.targetField.asFlinkSQLExpr

        case Coalesce(es) =>
          val columns = es.map(_.asFlinkSQLExpr)
          columns.find(_.isNotNull == true).get

        case c: CaseExpr => ???
      }

    }
  }

}

@scala.annotation.varargs
class GetKeys(propertyKeys: String*) extends ScalarFunction {

  @scala.annotation.varargs
  def eval(properties: AnyRef*): Array[String] = {
    propertyKeys.zip(properties).collect {
      case (key, value) if value != null => key
    }.toArray
  }

}

@scala.annotation.varargs
class GetTypes(relType: String*) extends ScalarFunction {

  @scala.annotation.varargs
  def eval(relTypeFlag: Boolean*): String = {
    relType.zip(relTypeFlag).collectFirst {
      case (tpe, true) => tpe
    }.orNull
  }
}

class Unwind(list: Array[Any]) extends TableFunction[AnyRef] {

  def eval(list: Array[AnyRef]): Unit = {
    list.foreach(collect)
  }

}
