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
package org.opencypher.morpheus.impl

import org.apache.spark.sql.catalyst.expressions.{ArrayAggregate, ArrayExists, ArrayFilter, ArrayTransform, CaseWhen, ExprId, LambdaFunction, Literal, NamedLambdaVariable, StringSplit}
import org.apache.spark.sql.functions.{array_contains => _, translate => _, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.opencypher.morpheus.impl.MorpheusFunctions._
import org.opencypher.morpheus.impl.convert.SparkConversions._
import org.opencypher.morpheus.impl.expressions.AddPrefix._
import org.opencypher.morpheus.impl.expressions.EncodeLong._
import org.opencypher.morpheus.impl.expressions.PercentileUdafs
import org.opencypher.morpheus.impl.temporal.TemporalConversions._
import org.opencypher.morpheus.impl.temporal.{TemporalUdafs, TemporalUdfs}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader

final case class SparkSQLMappingException(msg: String) extends InternalException(msg)

object SparkSQLExprMapper {

  implicit class RichExpression(expr: Expr) {

    /**
      * Converts `expr` with the `withConvertedChildren` function, which is passed the converted child expressions as its
      * argument.
      *
      * Iff the expression has `expr.nullInNullOut == true`, then any child being mapped to `null` will also result in
      * the parent expression being mapped to null.
      *
      * For these expressions the `withConvertedChildren` function is guaranteed to not receive any `null`
      * values from the evaluated children.
      */
    def nullSafeConversion(expr: Expr)(withConvertedChildren: Seq[Column] => Column)
      (implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {
      if (expr.cypherType == CTNull) {
        NULL_LIT
      } else if (expr.cypherType == CTTrue) {
        TRUE_LIT
      } else if (expr.cypherType == CTFalse) {
        FALSE_LIT
      } else {
        val evaluatedArgs = expr.children.map(_.asSparkSQLExpr)
        val withConvertedChildrenResult = withConvertedChildren(evaluatedArgs).expr
        if (expr.children.nonEmpty && expr.nullInNullOut && expr.cypherType.isNullable) {
          val nullPropagationCases = evaluatedArgs.map(_.isNull.expr).zip(Seq.fill(evaluatedArgs.length)(NULL_LIT.expr))
          new Column(CaseWhen(nullPropagationCases, withConvertedChildrenResult))
        } else {
          new Column(withConvertedChildrenResult)
        }
      }
    }

    /**
      * Attempts to create a Spark SQL expression from the Morpheus expression.
      *
      * @param header     the header of the [[MorpheusRecords]] in which the expression should be evaluated.
      * @param df         the dataframe containing the data over which the expression should be evaluated.
      * @param parameters query parameters
      * @return Some Spark SQL expression if the input was mappable, otherwise None.
      */
    def asSparkSQLExpr(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {
      val outCol = expr match {
        case v: LambdaVar =>
          val sparkType = v.cypherType.toSparkType.getOrElse(throw IllegalStateException(s"No valid dataType for LambdaVar $v"))
          new Column(NamedLambdaVariable(v.name, sparkType, nullable = v.cypherType.isNullable, ExprId(v.hashCode.toLong)))
        // Evaluate based on already present data; no recursion
        case _: Var | _: HasLabel | _: HasType | _: StartNode | _: EndNode => column_for(expr)
        // Evaluate bottom-up
        case _ => nullSafeConversion(expr)(convert)
      }
      header.getColumn(expr) match {
        case None => outCol
        case Some(colName) => outCol.as(colName)
      }
    }

    private def convert(convertedChildren: Seq[Column])
      (implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {

      def child0: Column = convertedChildren.head

      def child1: Column = convertedChildren(1)

      def child2: Column = convertedChildren(2)

      expr match {
        case _: ListLit => array(convertedChildren: _*)
        case l: Lit[_] => lit(l.v)
        case _: AliasExpr => child0
        case Param(name) => parameters(name).toSparkLiteral

        // Predicates
        case _: Equals => child0 === child1
        case _: Not => !child0
        case Size(e) => {
          e.cypherType match {
            case CTString => length(child0)
            case _ => size(child0) // it's a list
          }
        }.cast(LongType)
        case _: Ands => convertedChildren.foldLeft(TRUE_LIT)(_ && _)
        case _: Ors => convertedChildren.foldLeft(FALSE_LIT)(_ || _)
        case _: IsNull => child0.isNull
        case _: IsNotNull => child0.isNotNull
        case _: Exists => child0.isNotNull
        case _: LessThan => child0 < child1
        case _: LessThanOrEqual => child0 <= child1
        case _: GreaterThanOrEqual => child0 >= child1
        case _: GreaterThan => child0 > child1

        case _: StartsWith => child0.startsWith(child1)
        case _: EndsWith => child0.endsWith(child1)
        case _: Contains => child0.contains(child1)
        case _: RegexMatch => regex_match(child0, child1)

        // Other
        case Explode(list) => list.cypherType match {
          case CTNull => explode(NULL_LIT.cast(ArrayType(NullType)))
          case _ => explode(child0)
        }

        case _: ElementProperty => if (!header.contains(expr)) NULL_LIT else column_for(expr)
        case MapProperty(_, key) => if (expr.cypherType.material == CTVoid) NULL_LIT else child0.getField(key.name)
        case DateProperty(_, key) => temporalAccessor[java.sql.Date](child0, key.name)
        case LocalDateTimeProperty(_, key) => temporalAccessor[java.sql.Timestamp](child0, key.name)
        case DurationProperty(_, key) => TemporalUdfs.durationAccessor(key.name.toLowerCase).apply(child0)

        case LocalDateTime(maybeDateExpr) => maybeDateExpr.map(e => lit(e.resolveTimestamp).cast(DataTypes.TimestampType)).getOrElse(current_timestamp())
        case Date(maybeDateExpr) => maybeDateExpr.map(e => lit(e.resolveDate).cast(DataTypes.DateType)).getOrElse(current_timestamp())
        case Duration(durationExpr) => lit(durationExpr.resolveInterval)

        case In(lhs, rhs) => rhs.cypherType.material match {
          case CTList(inner) if inner.couldBeSameTypeAs(lhs.cypherType) => array_contains(child1, child0)
          case _ => NULL_LIT
        }

        //list-access
        case _: Head => element_at(child0, 1)
        case _: Last => element_at(child0, -1)

        // Arithmetic
        case Add(lhs, rhs) =>
          val lhsCT = lhs.cypherType.material
          val rhsCT = rhs.cypherType.material
          lhsCT -> rhsCT match {
            case (CTList(lhInner), CTList(rhInner)) =>
              if ((lhInner | rhInner).isSparkCompatible) {
                concat(child0, child1)
              } else {
                throw SparkSQLMappingException(s"Lists of different inner types are not supported (${lhInner.material}, ${rhInner.material})")
              }
            case (CTList(inner), nonListType) if (inner | nonListType).isSparkCompatible => concat(child0, array(child1))
            case (nonListType, CTList(inner)) if (inner | nonListType).isSparkCompatible => concat(array(child0), child1)
            case (CTString, _) if rhsCT.subTypeOf(CTNumber) => concat(child0, child1.cast(StringType))
            case (_, CTString) if lhsCT.subTypeOf(CTNumber) => concat(child0.cast(StringType), child1)
            case (CTString, CTString) => concat(child0, child1)
            case (CTDate, CTDuration) => TemporalUdfs.dateAdd(child0, child1)
            case _ => child0 + child1
          }

        case Subtract(lhs, rhs) if lhs.cypherType.material.subTypeOf(CTDate) && rhs.cypherType.material.subTypeOf(CTDuration) =>
          TemporalUdfs.dateSubtract(child0, child1)

        case _: Subtract => child0 - child1

        case _: Multiply => child0 * child1
        case div: Divide => (child0 / child1).cast(div.cypherType.getSparkType)

        // Id functions
        case _: Id => child0
        case PrefixId(_, prefix) => child0.addPrefix(lit(prefix))
        case ToId(e) =>
          e.cypherType.material match {
            case CTInteger => child0
//            case ct if ct.toSparkType.contains(BinaryType) => child0
            case other => throw IllegalArgumentException("a type that may be converted to an ID", other)
          }

        // Functions
        case _: MonotonicallyIncreasingId => monotonically_increasing_id()
        case Labels(e) =>
          val possibleLabels = header.labelsFor(e.owner.get).toSeq.sortBy(_.label.name)
          val labelBooleanFlagsCol = possibleLabels.map(_.asSparkSQLExpr)
          val nodeLabels = filter_true(possibleLabels.map(_.label.name), labelBooleanFlagsCol)
          nodeLabels

        case Type(e) =>
          val possibleRelTypes = header.typesFor(e.owner.get).toSeq.sortBy(_.relType.name)
          val relTypeBooleanFlagsCol = possibleRelTypes.map(_.asSparkSQLExpr)
          val relTypes = filter_true(possibleRelTypes.map(_.relType.name), relTypeBooleanFlagsCol)
          val relType = get_array_item(relTypes, index = 0)
          relType

        case Keys(e) =>
          e.cypherType.material match {
            case element if element.subTypeOf(CTElement) =>
              val possibleProperties = header.propertiesFor(e.owner.get).toSeq.sortBy(_.key.name)
              val propertyNames = possibleProperties.map(_.key.name)
              val propertyValues = possibleProperties.map(_.asSparkSQLExpr)
              filter_not_null(propertyNames, propertyValues)

            case CTMap(inner) =>
              val mapColumn = child0
              val (propertyKeys, propertyValues) = inner.keys.map { e =>
                // Whe have to make sure that every column has the same type (true or null)
                e -> when(mapColumn.getField(e).isNotNull, TRUE_LIT).otherwise(NULL_LIT)
              }.toSeq.unzip
              filter_not_null(propertyKeys, propertyValues)

            case other => throw IllegalArgumentException("an Expression with type CTNode, CTRelationship or CTMap", other)
          }

        case Properties(e) =>
          e.cypherType.material match {
            case element if element.subTypeOf(CTElement) =>
              val propertyExpressions = header.propertiesFor(e.owner.get).toSeq.sortBy(_.key.name)
              val propertyColumns = propertyExpressions
                .map(propertyExpression => propertyExpression.asSparkSQLExpr.as(propertyExpression.key.name))
              create_struct(propertyColumns)
            case _: CTMap => child0
            case other =>
              throw IllegalArgumentException("a node, relationship or map", other, "Invalid input to properties function")
          }

        case StartNodeFunction(e) => header.startNodeFor(e.owner.get).asSparkSQLExpr
        case EndNodeFunction(e) => header.endNodeFor(e.owner.get).asSparkSQLExpr

        case _: ToFloat => child0.cast(DoubleType)
        case _: ToInteger => child0.cast(IntegerType)
        case _: ToString => child0.cast(StringType)
        case _: ToBoolean => child0.cast(BooleanType)

        case _: Trim => trim(child0)
        case _: LTrim => ltrim(child0)
        case _: RTrim => rtrim(child0)
        case _: Reverse => reverse(child0)
        case _: ToUpper => upper(child0)
        case _: ToLower => lower(child0)

        case _: Range => sequence(child0, child1, convertedChildren.lift(2).getOrElse(ONE_LIT))
        case _: Replace => translate(child0, child1, child2)
        case _: Substring => child0.substr(child1 + ONE_LIT, convertedChildren.lift(2).getOrElse(length(child0) - child1))
        case _: Split => new Column(StringSplit(child0.expr, child1.expr))

        // Mathematical functions
        case E => E_LIT
        case Pi => PI_LIT

        case _: Sqrt => sqrt(child0)
        case _: Log => log(child0)
        case _: Log10 => log(10.0, child0)
        case _: Exp => exp(child0)
        case _: Abs => abs(child0)
        case _: Ceil => ceil(child0).cast(DoubleType)
        case _: Floor => floor(child0).cast(DoubleType)
        case Rand => rand()
        case _: Round => round(child0).cast(DoubleType)
        case _: Sign => signum(child0).cast(IntegerType)

        case _: Acos => acos(child0)
        case _: Asin => asin(child0)
        case _: Atan => atan(child0)
        case _: Atan2 => atan2(child0, child1)
        case _: Cos => cos(child0)
        case Cot(e) => Divide(IntegerLit(1), Tan(e)).asSparkSQLExpr
        case _: Degrees => degrees(child0)
        case Haversin(e) => Divide(Subtract(IntegerLit(1), Cos(e)), IntegerLit(2)).asSparkSQLExpr
        case _: Radians => radians(child0)
        case _: Sin => sin(child0)
        case _: Tan => tan(child0)

        // Time functions
        case Timestamp => current_timestamp().cast(LongType)

        // Bit operations
        case _: BitwiseAnd => child0.bitwiseAND(child1)
        case _: BitwiseOr => child0.bitwiseOR(child1)
        case ShiftLeft(_, IntegerLit(shiftBits)) => shiftLeft(child0, shiftBits.toInt)
        case ShiftRightUnsigned(_, IntegerLit(shiftBits)) => shiftRightUnsigned(child0, shiftBits.toInt)

        // Pattern Predicate
        case ep: ExistsPatternExpr => ep.targetField.asSparkSQLExpr

        case Coalesce(es) =>
          val columns = es.map(_.asSparkSQLExpr)
          coalesce(columns: _*)

        case CaseExpr(_, maybeDefault) =>
          val (maybeConvertedDefault, convertedAlternatives) = if (maybeDefault.isDefined) {
            Some(convertedChildren.head) -> convertedChildren.tail
          } else {
            None -> convertedChildren
          }
          val indexed = convertedAlternatives.zipWithIndex
          val conditions = indexed.collect { case (c, i) if i % 2 == 0 => c }
          val values = indexed.collect { case (c, i) if i % 2 == 1 => c }
          val branches = conditions.zip(values)
          switch(branches, maybeConvertedDefault)

        case ContainerIndex(container, index) =>
          val containerCol = container.asSparkSQLExpr
          container.cypherType.material match {
            case c if c.subTypeOf(CTContainer) => containerCol.get(index.asSparkSQLExpr)
            case other => throw NotImplementedException(s"Accessing $other by index is not supported")
          }

        case _: ListSliceFromTo => list_slice(child0, Some(child1), Some(child2))
        case _: ListSliceFrom => list_slice(child0, Some(child1), None)
        case _: ListSliceTo => list_slice(child0, None, Some(child1))

        case ListComprehension(variable, innerPredicate, extractExpression, listExpr) =>
          val lambdaVar = variable.asSparkSQLExpr.expr match {
            case v: NamedLambdaVariable => v
            case err => throw IllegalStateException(s"$variable should be converted into a NamedLambdaVariable instead of $err")
          }
          val filteredExpr = innerPredicate match {
            case Some(filterExpr) =>
              val filterFunc = LambdaFunction(filterExpr.asSparkSQLExpr.expr, Seq(lambdaVar))
              ArrayFilter(listExpr.asSparkSQLExpr.expr, filterFunc)
            case None => listExpr.asSparkSQLExpr.expr
          }
          val result = extractExpression match{
            case Some(extractExpr) =>
              val extractFunc = LambdaFunction(extractExpr.asSparkSQLExpr.expr, Seq(lambdaVar))
              ArrayTransform(filteredExpr, extractFunc)
            case None => filteredExpr
          }
          new Column(result)

        case ListReduction(acc, v, _, _, _) =>
          val convertedChildrenExpr = convertedChildren.map(_.expr)
          val (initVar, accVar) = convertedChildrenExpr.slice(0, 2) match {
            case Seq(i: NamedLambdaVariable, a: NamedLambdaVariable) => i -> a
            case err => throw IllegalStateException(s"($v,$acc) should be converted into a NamedLambdaVariables instead of $err")
          }
          val reduceFunc = LambdaFunction(convertedChildrenExpr(2), Seq(initVar, accVar))
          val finishFunc = LambdaFunction(accVar, Seq(accVar))

          val reduceExpr = ArrayAggregate(convertedChildrenExpr(4), convertedChildrenExpr(3), reduceFunc, finishFunc)
          new Column(reduceExpr)

        case predExpr: IterablePredicateExpr =>
          val convertedChildrenExpr = convertedChildren.map(_.expr)
          val lambdaVar = child0.expr match {
            case v: NamedLambdaVariable => v
            case err => throw IllegalStateException(s"${predExpr.exprs.head} should be converted into a NamedLambdaVariable instead of $err")
          }
          val filterFunc = LambdaFunction(convertedChildrenExpr(1), Seq(lambdaVar))
          predExpr match {
            case _: ListAll =>
              val lengthBefore = size(convertedChildren(2))
              val filterExpr = ArrayFilter(convertedChildrenExpr(2), filterFunc)
              val lengthAfter = size(new Column(filterExpr))
              lengthBefore === lengthAfter
            case _: ListAny =>
              val resExpr = ArrayExists(convertedChildrenExpr(2), filterFunc)
              new Column(resExpr)
            case _: ListNone =>
              val resExpr = ArrayExists(convertedChildrenExpr(2), filterFunc)
              val resCol = new Column(resExpr)
              not(resCol)
            case _: ListSingle =>
              val filterExpr = ArrayFilter(convertedChildrenExpr(2), filterFunc)
              val lengthAfter = size(new Column(filterExpr))
              ONE_LIT === lengthAfter
          }

        case MapExpression(items) => expr.cypherType.material match {
          case CTMap(_) =>
            val innerColumns = items.map {
              case (key, innerExpr) => innerExpr.asSparkSQLExpr.as(key)
            }.toSeq
            create_struct(innerColumns)
          case other => throw IllegalArgumentException("an expression of type CTMap", other)
        }

        case MapProjection(mapOwner, items, includeAllProps) =>
          val convertedItems = items.map { case (key, value) => value.asSparkSQLExpr.as(key) }
          val itemKeys = items.map { case (propKey, _) => propKey }
          val intersectedMapItems = if (includeAllProps) {
            mapOwner.cypherType.material match {
              case x if x.subTypeOf(CTElement) =>
                val uniqueEntityProps = header.propertiesFor(mapOwner).filterNot(p => itemKeys.contains(p.key.name))
                val propertyColumns = uniqueEntityProps.map(p => p.asSparkSQLExpr.as(p.key.name))
                convertedItems ++ propertyColumns
              case CTMap(inner) =>
                val uniqueMapKeys = inner.keys.filterNot(key => itemKeys.contains(key))
                val uniqueMapColumns = uniqueMapKeys.map(key => child0.getField(key).as(key))
                convertedItems ++ uniqueMapColumns
            }
          }
          else {
            convertedItems
          }
          create_struct(intersectedMapItems)

        // Aggregators
        case Count(_, distinct) =>
          if (distinct) countDistinct(child0)
          else count(child0)

        case Collect(_, distinct) =>
          if (distinct) collect_set(child0)
          else collect_list(child0)

        case CountStar => count(ONE_LIT)
        case _: Avg =>
          expr.cypherType match {
            case CTDuration => TemporalUdafs.durationAvg(child0)
            case _ => avg(child0)
          }
        case _: Max =>
          expr.cypherType match {
            case CTDuration => TemporalUdafs.durationMax(child0)
            case _ => max(child0)
          }
        case _: Min =>
          expr.cypherType match {
            case CTDuration => TemporalUdafs.durationMin(child0)
            case _ => min(child0)
          }
        case _: PercentileCont =>
          val percentile = child1.expr match {
            case Literal(v, DoubleType) => v.asInstanceOf[Double]
            case _ => throw IllegalArgumentException("Literal as percentage for percentileCont()", child1.expr)
          }
          PercentileUdafs.percentileCont(percentile)(child0)
        case _:PercentileDisc =>
          val percentile = child1.expr match {
            case Literal(v, DoubleType) => v.asInstanceOf[Double]
            case _ => throw IllegalArgumentException("Literal as percentage for percentileDisc()", child1.expr)
          }
          PercentileUdafs.percentileDisc(percentile, child0.expr.dataType)(child0)
        case _: StDev => stddev(child0)
        case _: StDevP => stddev_pop(child0)
        case _: Sum =>
          expr.cypherType match {
            case CTDuration => TemporalUdafs.durationSum(child0)
            case _ => sum(child0)
          }


        case BigDecimal(_, precision, scale) =>
          make_big_decimal(child0, precision.toInt, scale.toInt)

        case _ =>
          throw NotImplementedException(s"No support for converting Cypher expression $expr to a Spark SQL expression")
      }
    }

  }

}
