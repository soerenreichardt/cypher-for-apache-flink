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
package org.opencypher.flink.impl.table

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions
import org.apache.flink.table.expressions.{Expression, UnresolvedFieldReference}
import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.impl.FlinkSQLExprMapper._
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink.impl.convert.FlinkConversions._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException, UnsupportedOperationException}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.graph.RelationalCypherSession
import org.opencypher.okapi.relational.api.table.{Table => RelationalTable}
import org.opencypher.okapi.relational.impl.planning._
import org.opencypher.okapi.relational.impl.table.RecordHeader

object FlinkCypherTable {

  implicit class FlinkTable(val table: Table) extends RelationalTable[FlinkTable] {

    private case class EmptyRow()

    override def physicalColumns: Seq[String] = table.getSchema.getFieldNames

    override def columnType: Map[String, CypherType] = physicalColumns.map(c => c -> table.cypherTypeForColumn(c)).toMap

    override def rows: Iterator[String => CypherValue] = table.collect().iterator.map { row =>
      physicalColumns.map(c => c -> CypherValue(row.getField(table.getSchema.columnNameToIndex(c)))).toMap
    }

    override def size: Long = table.count()

    override def select(cols: String*): FlinkTable = {
      table.select(cols.map(UnresolvedFieldReference): _*)
    }

    override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherValue.CypherMap): FlinkTable = {
      table.filter(expr.asFlinkSQLExpr(header, table, parameters))
    }

    override def limit(n: Long): FlinkTable = table.fetch(n.toInt)

    override def skip(n: Long): FlinkTable = table.offset(n.toInt)

    override def show(rows: Int): Unit = ???

    override def withColumns(columns: (Expr, String)*)
      (implicit header: RecordHeader, parameters: CypherMap): FlinkTable = {
      val initialColumnNameToFieldReference: Map[String, Expression] =
        table.columns.map(c => c -> UnresolvedFieldReference(c)).toMap
      val updatedColumns = columns.foldLeft(initialColumnNameToFieldReference) { case (columnMap, (expr, columnName)) =>
        val column = expr.asFlinkSQLExpr(header, table, parameters).as(Symbol(columnName))
        columnMap + (columnName -> column)
      }
      val existingColumnNames = table.columns
      val columnsForSelect = existingColumnNames.map(updatedColumns) ++
        updatedColumns.filterKeys(!existingColumnNames.contains(_)).values

      table.select(columnsForSelect: _*)
    }

    override def drop(cols: String*): FlinkTable = {
      val columnsLeft = table.physicalColumns.diff(cols)
      select(columnsLeft: _*)
    }

    override def orderBy(sortItems: (Expr, Order)*)(implicit header: RecordHeader, paramaters: CypherMap): FlinkTable = {
      val mappedSortItems = sortItems.map { case (expr, order) =>
        val mappedExpr = expr.asFlinkSQLExpr(header, table, paramaters)
          order match {
            case Ascending => mappedExpr.asc
            case Descending => mappedExpr.desc
          }
      }
      table.orderBy(mappedSortItems: _*)
    }

    override def group(by: Set[Var], aggregations: Set[(Aggregator, (String, CypherType))])
      (implicit header: RecordHeader, parameters: CypherMap): FlinkTable = {

      def withInnerExpr(expr: Expr)(f: Expression => Expression) =
        f(expr.asFlinkSQLExpr(header, table, parameters))

      val columns =
        if (by.nonEmpty) {
          by.flatMap { expr =>
            val withChildren = header.ownedBy(expr)
            withChildren.map(e => withInnerExpr(e)(identity))
          }
        } else null

      val data =
        if (columns != null) {
          Left(table.groupBy(columns.toSeq: _*))
        } else Right(table)

      val flinkAggFunctions = aggregations.map {
        case (aggFunc, (colName, cypherType)) =>
          val columnName = Symbol(colName)
          aggFunc match {
            case Avg(expr) =>
              withInnerExpr(expr)(
                _.avg
                  .cast(aggFunc.cypherType.getFlinkType)
                  .as(columnName)
              )

            case CountStar(_) =>
              withInnerExpr(IntegerLit(0)(CTInteger))(_.count.as(columnName))

            case Count(expr, _) => withInnerExpr(expr)( column =>
              column.count
                .as(columnName))

            case Max(expr) =>
              withInnerExpr(expr)(_.max.as(columnName))

            case Min(expr) =>
              withInnerExpr(expr)(_.min.as(columnName))

            case Sum(expr) =>
              withInnerExpr(expr)(_.sum.as(columnName))

            case Collect(expr, _) => withInnerExpr(expr) { column =>
              val list = array(column)
              list as columnName
            }

            case x =>
              throw NotImplementedException(s"Aggregation function $x")
          }
      }

      data.fold(
        _.select((columns ++ flinkAggFunctions).toSeq: _*),
        _.select(flinkAggFunctions.toSeq: _*)
      )
    }

    override def unionAll(other: FlinkTable): FlinkTable = {
      val leftTypes = table.getSchema.getFieldTypes.flatMap(_.toCypherType())
      val rightTypes = other.table.getSchema.getFieldTypes.flatMap(_.toCypherType())

      leftTypes.zip(rightTypes).foreach {
        case (leftType, rightType) if !leftType.nullable.couldBeSameTypeAs(rightType.nullable) =>
          throw IllegalArgumentException(
            "Equal column types for union all (differing nullability is OK)",
            s"Left fields: ${table.getSchema.getFieldTypes.mkString(", ")}\n\tRight fields: ${other.table.getSchema.getFieldTypes.mkString(", ")}"
          )
        case _ =>
      }

      table.withCypherCompatibleTypes.union(other.table.withCypherCompatibleTypes)
    }

    override def join(other: FlinkTable, joinType: JoinType, joinCols: (String, String)*): FlinkTable = {

      val overlap = this.physicalColumns.toSet.intersect(other.physicalColumns.toSet)
      assert(overlap.isEmpty, s"overlapping columns: $overlap")

      val joinExpr = joinCols.map {
        case (l, r) => UnresolvedFieldReference(l) === UnresolvedFieldReference(r)
      }.foldLeft(expressions.Literal(true, Types.BOOLEAN): Expression) { (acc, expr) => acc && expr }

      joinType match {
        case InnerJoin => table.join(other.table, joinExpr)
        case LeftOuterJoin => table.leftOuterJoin(other.table, joinExpr)
        case RightOuterJoin => table.rightOuterJoin(other.table, joinExpr)
        case FullOuterJoin => table.fullOuterJoin(other.table, joinExpr)
        case CrossJoin => throw IllegalStateException("This should never happen, cross joins resolve to a cross operator.")
      }
    }

    override def cross(other: FlinkTable)(implicit session: RelationalCypherSession[FlinkTable]): FlinkTable = {
      implicit val capf = session.asInstanceOf[CAPFSession]
      table.cross(other.table)
    }

    override def distinct: FlinkTable =
      table.distinct()

    override def distinct(cols: String*): FlinkTable =
      table.distinct()

    override def withColumnRenamed(oldColumn: String, newColumn: String): FlinkTable =
      table.safeRenameColumn(oldColumn, newColumn)

    override def columnsFor(returnItem: String): Set[String] =
      throw UnsupportedOperationException("A FlinkTable does not have return items")
  }

}
