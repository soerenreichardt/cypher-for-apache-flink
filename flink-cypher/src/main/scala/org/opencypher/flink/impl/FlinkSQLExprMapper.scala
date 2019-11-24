/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
import org.apache.flink.table.expressions.{Expression, MapConstructor}
import org.apache.flink.table.functions.ScalarFunction
import org.opencypher.flink.impl.CAPFFunctions._
import org.opencypher.flink.impl.convert.FlinkConversions._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader

object FlinkSQLExprMapper {

  implicit class RichExpression(expr: Expr) {

    def asFlinkSQLExpr(implicit header: RecordHeader, table: Table, parameters: CypherMap): Expression = {

      val outExpr = expr match {
        case _: Var | _: HasLabel | _: HasType | _: StartNode | _: EndNode => expression_for(expr)
        case _ => null_safe_conversion(expr)(convert)
      }
      header.getColumn(expr) match {
        case None => outExpr
        case Some(colName) => outExpr.as(Symbol(colName))
      }
    }

    private def convert(convertedChildren: Seq[Expression])
      (implicit header: RecordHeader, table: Table, parameters: CypherMap): Expression = {

      def child0: Expression = convertedChildren.head

      def child1: Expression = convertedChildren(1)

      def child2: Expression = convertedChildren(2)

      expr match {

        case _: ListLit => array(convertedChildren.head, convertedChildren.tail: _*)
        case n: NullLit => expressions.Literal(null, n.cypherType.getFlinkType)
        case l: Lit[_] => expressions.Literal(l.v, TypeInformation.of(l.v.getClass))
        case _: AliasExpr => child0
        case Param(name) =>parameters(name).toFlinkLiteral

          // Predicates
        case _: Equals => child0 === child1
        case _: Not => !child0
        case Size(e) => {
          e.cypherType match {
            case CTString => child0.charLength()
            case _ => child0.cardinality()
          }
        }.cast(Types.LONG)
        case _: Ands => convertedChildren.foldLeft(expressions.Literal(true, Types.BOOLEAN): Expression)(_ && _)
        case _: Ors => convertedChildren.foldLeft(expressions.Literal(false, Types.BOOLEAN): Expression)(_ || _)
        case _: IsNull => child0.isNull
        case _: IsNotNull => child0.isNotNull
        case _: Exists => child0.isNotNull
        case _: LessThan => child0 < child1
        case _: LessThanOrEqual => child0 <= child1
        case _: GreaterThan => child0 > child1
        case _: GreaterThanOrEqual => child0 >= child1

        case _: StartsWith => throw NotImplementedException("StartsWith expression")
        case _: EndsWith => throw NotImplementedException("EndsWithExpression")
        case _: Contains => throw NotImplementedException("Contains expression")
        case _: RegexMatch => child0.regexpExtract(child1)

        case Explode(list) => list.asFlinkSQLExpr

        case ElementProperty(e, PropertyKey(key)) =>
          e.cypherType.material match {
            case CTMap(inner) => if (inner.keySet.contains(key)) child0.get(key) else null_lit(e.cypherType.getFlinkType)
            case _ =>
              if (!header.contains(expr)) {
                null_lit(expr.cypherType.getFlinkType)
              } else {
                expression_for(expr)
              }
          }

        case In(lhs, rhs) => rhs.cypherType.material match {
          case CTList(CTVoid) => FALSE_LIT
          case CTList(inner) if inner.couldBeSameTypeAs(lhs.cypherType) => child0 in child1
          case _ => null_lit(lhs.cypherType.getFlinkType)
        }

        case Add(lhs, rhs) =>
          val lhsCT = lhs.cypherType.material
          val rhsCT = rhs.cypherType.material
          lhsCT -> rhsCT match {
            case (CTString, _) if rhsCT.subTypeOf(CTNumber) => concat(child0, child1.cast(Types.STRING))
            case (_, CTString) if lhsCT.subTypeOf(CTNumber) => concat(child0.cast(Types.STRING), child1)
            case (CTString, CTString) => concat(child0, child1)
            case _ => child0 + child1
          }

        case _: Subtract => child0 - child1
        case _: Multiply => child0 * child1
        case div: Divide => (child0 / child1).cast(div.cypherType.getFlinkType)

        case _: Id => child0

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

        case Properties(e) =>
          e.cypherType.material match {
            case _: CTNode | _: CTRelationship =>
              val propertyExpressions = header.propertiesFor(e.owner.get).toSeq.sortBy(_.key.name)
              val propertyColumns = propertyExpressions
                .map(propertyExpression => propertyExpression.asFlinkSQLExpr.as(Symbol(propertyExpression.key.name)))
              array(propertyColumns.head, propertyColumns.tail: _*)
            case _: CTMap => child0
            case other =>
              throw IllegalArgumentException("a node, relationship or map", other, "Invalid input to properties function")
          }

        case StartNodeFunction(e) => header.startNodeFor(e.owner.get).asFlinkSQLExpr
        case EndNodeFunction(e) => header.endNodeFor(e.owner.get).asFlinkSQLExpr

        case _: ToFloat => child0.cast(Types.DOUBLE)
        case _: ToInteger => child0.cast(Types.INT)
        case _: ToString => child0.cast(Types.STRING)
        case _: ToBoolean => child0.cast(Types.BOOLEAN)

        case _: Trim => child0.trim()
        case _: LTrim => child0.ltrim
        case _: RTrim => child0.rtrim
        case _: ToUpper => child0.upperCase
        case _: ToLower => child0.lowerCase

        case _: Replace => child0.regexpReplace(child1, child2)

        case _: Substring => child0.substring(child1 + ONE_LIT, convertedChildren.lift(2).getOrElse(ONE_LIT))

        case E => E_LIT
        case Pi => PI_LIT

        case _: Sqrt => child0.sqrt
        case _: Log => child0.log
        case _: Log10 => child0.log10
        case _: Exp => child0.exp
        case _: Abs => child0.abs
        case _: Ceil => child0.ceil
        case _: Floor => child0.floor
        case Rand => rand()
        case _: Round => child0.round(???) // TODO: move the round argument to OKAPI
        case _: Sign => child0.sign

        case _: Acos => child0.acos
        case _: Asin => child0.asin
        case _: Atan => child0.atan
        case _: Atan2 => atan2(child0, child1)
        case _: Cos => child0.cos
        case Cot(e) => Divide(IntegerLit(1), Tan(e)).asFlinkSQLExpr
        case _: Degrees => child0.degrees
        case Haversin(e) => Divide(Subtract(IntegerLit(1), Cos(e)), IntegerLit(2)).asFlinkSQLExpr
        case _: Radians => child0.radians
        case _: Sin => child0.sin
        case _: Tan => child0.tan

        case _: StDev => child0.stddevSamp
        case _: StDevP => child0.stddevPop

        case ep: ExistsPatternExpr => ep.targetField.asFlinkSQLExpr

        case c@Coalesce(es) =>
          val columns = es.map(_.asFlinkSQLExpr)
          val tpe = c.cypherType.toFlinkType.get
          val normalizedColumns: List[Expression] = columns.map(_.cast(tpe))

          def coalesceFromIfElse(columnsTail: IndexedSeq[Expression]): Expression = {
            columnsTail.headOption match {
              case Some(head) => head.isNotNull ? (head, coalesceFromIfElse(columnsTail.tail))
              case None => expressions.Null(tpe)
            }
          }

          coalesceFromIfElse(normalizedColumns.toIndexedSeq)

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

          expressions.If(alternativesHead._1, alternativesHead._2, ifElseExpression(alternatives.tail.toIndexedSeq))

        case ContainerIndex(container, index) =>
          val indexExpression = index.asFlinkSQLExpr
          val containerExpression = container.asFlinkSQLExpr

          container.cypherType.material match {
            case _: CTList | _: CTMap => containerExpression.at(indexExpression)
            case other => throw NotImplementedException(s"Accessing $other by index is not supported")
          }

        case MapExpression(items) => expr.cypherType.material match {
          case CTMap(_) =>
            val innerColumns = items.map {
              case (key, innerExpr) => innerExpr.asFlinkSQLExpr.as(Symbol(key))
            }.toSeq
            MapConstructor(innerColumns)
          case other => throw IllegalArgumentException("an expression pf type CTMap", other)
        }

        // Aggregators
        case CountStar => expressions.Count(header.columns.head)
        case Count(_, _) => child0.count
        case Collect(_, _) => child0.collect
        case _: Avg => child0.avg
        case _: Max => child0.max
        case _: Min => child0.min
        case _: Sum => child0.sum

        case _ =>
          throw NotImplementedException(s"No support for converting Cypher expression $expr to a Flink Table expression")
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
