/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.flink.impl

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.ScalarFunction
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink.impl.convert.FlinkConversions._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherList, CypherMap}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader

object FlinkSQLExprMapper {

  implicit class RichExpression(expr: Expr) {

    def verify(implicit header: RecordHeader): Unit = {
      if (header.expressionsFor(expr).isEmpty) throw IllegalStateException(s"No slot for expression $expr")
    }

    def compare(comparator: Expression => (Expression => Expression), lhs: Expr, rhs: Expr)
      (implicit header: RecordHeader, table: Table, parameters: CypherMap): Expression = {
      comparator(lhs.asFlinkSQLExpr)(rhs.asFlinkSQLExpr)
    }

    def lt(e: Expression): Expression => Expression = e < _

    def lteq(e: Expression): Expression => Expression = e <= _

    def gt(e: Expression): Expression => Expression = e > _

    def gteq(e: Expression): Expression => Expression = e >= _

    def asFlinkSQLExpr(implicit header: RecordHeader, table: Table, parameters: CypherMap): Expression = {

      expr match {

        case p@Param(name) if p.cypherType.subTypeOf(CTList(CTAny)).maybeTrue =>
          parameters(name) match {
            case CypherList(l) =>
              val expressionList = l.unwrap.map( elem => expressions.Literal(elem, TypeInformation.of(elem.getClass)))
              if (expressionList.isEmpty)
                throw new NotImplementedException("Empty lists are not yet supported by Flink")

              array(expressionList.head, expressionList.tail: _*)
            case notAList => throw IllegalArgumentException("a Cypher list", notAList)
          }
        case Param(name) =>
          expressions.Literal(parameters(name).unwrap, TypeInformation.of(parameters(name).unwrap.getClass))

        case _: Var | _: Param | _:Property | _: HasLabel | _: HasType | _: StartNode | _: EndNode =>
          verify

          val colName = header.column(expr)
          if (table.columns.contains(colName)) {
            expressions.UnresolvedFieldReference(colName)
          } else {
            expressions.Null(expr.cypherType.getFlinkType) as Symbol(colName)
          }

        case AliasExpr(innerExpr, _) =>
          innerExpr.asFlinkSQLExpr

        case ListLit(exprs) =>
          val flinkExpressions = exprs.map(_.asFlinkSQLExpr)
          array(flinkExpressions.head, flinkExpressions.tail: _*)

        case DateTime(expr) => {
          expr match {
            case Some(e) => e.asFlinkSQLExpr.toTimestamp
            case None => currentTimestamp()
          }
        }

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
        case Labels(e)=>
          val node = e.owner.get
          val labelExprs = header.labelsFor(node)
          val (labelNames, labelColumns) = labelExprs
            .toSeq
            .map(e => e.label.name -> e.asFlinkSQLExpr)
            .sortBy(_._1)
            .unzip
          val getLabelsFunction = new GetLabels(labelNames: _*)
          getLabelsFunction(labelColumns: _*)

        case Keys(e) =>
          val node = e.owner.get
          val propertyExprs = header.propertiesFor(node).toSeq.sortBy(_.key.name)
          val (propertyKeys, propertyColumns) = propertyExprs.map(e => e.key.name -> e.asFlinkSQLExpr).unzip

          val getKeysFunction = new GetKeys(propertyKeys: _*)
          getKeysFunction(propertyColumns: _*)

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

        case Explode(list) =>
          list.asFlinkSQLExpr

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

        case c: CaseExpr =>
          val alternatives = c.alternatives.map {
            case (predicate, action) => (predicate.asFlinkSQLExpr, action.asFlinkSQLExpr)
          }

          val alternativesHead = alternatives.head

          def ifElseExpression(tailExprs: IndexedSeq[(Expression, Expression)]): Expression = {
            tailExprs.headOption match {
              case Some(head) => expressions.If(head._1, head._2, ifElseExpression(tailExprs.tail))
              case None =>
                c.default match {
                  case Some(inner) => inner.asFlinkSQLExpr
                  case None => expressions.Null(Types.LONG)
                }
            }
          }

          expressions.If(alternativesHead._1, alternativesHead._2, ifElseExpression(alternatives.tail))
      }

    }
  }

}

@scala.annotation.varargs
class GetTypes(relTypes: String*) extends ScalarFunction {

  @scala.annotation.varargs
  def eval(relTypeFlag: Boolean*): String = {
    relTypes.zip(relTypeFlag).collectFirst {
      case (tpe, true) => tpe
    }.orNull
  }
}

@scala.annotation.varargs
class GetLabels(labels: String*) extends ScalarFunction {

  @scala.annotation.varargs
  def eval(labelFlag: Boolean*): Array[String] = {
    labels.zip(labelFlag).collect {
      case (label, true) => label
    }.toArray
  }
}

@scala.annotation.varargs
class GetKeys(keys: String*) extends ScalarFunction {

  @scala.annotation.varargs
  def eval(valueColumns: Any*): Array[String] = {
    keys.zip(valueColumns).collect {
      case (key, true) => key
    }.toArray
  }
}
