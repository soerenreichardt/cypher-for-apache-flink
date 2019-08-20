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

import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions.{Expression, MapConstructor, Null, UnresolvedFieldReference}
import org.apache.flink.table.expressions
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.relational.impl.table.RecordHeader
import TableOps._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.opencypher.flink.impl.convert.FlinkConversions._
import org.opencypher.okapi.api.types.CTNull
import org.opencypher.okapi.api.value.CypherValue.{CypherList, CypherMap, CypherValue}
import org.opencypher.okapi.api.types.CypherType._
import FlinkSQLExprMapper._
import org.apache.flink.table.api.scala.array

object CAPFFunctions {

  val FALSE_LIT = expressions.Literal(false, Types.BOOLEAN)
  val TRUE_LIT = expressions.Literal(true, Types.BOOLEAN)
  val ONE_LIT = expressions.Literal(1, Types.INT)
  val E_LIT = expressions.E()
  val PI_LIT = expressions.Pi()
  def null_lit(tpe: TypeInformation[_] = Types.BOOLEAN) = Null(tpe)

  def null_safe_conversion(expr: Expr)(withConvertedChildren: Seq[Expression] => Expression)
    (implicit header: RecordHeader, table: Table, parameters: CypherMap): Expression = {
    if (expr.cypherType == CTNull) {
      null_lit(expr.cypherType.getFlinkType)
    } else {
      val evaluatedArgs = expr.children.map(_.asFlinkSQLExpr)
      withConvertedChildren(evaluatedArgs)
    }
  }

  def expression_for(expr: Expr)(implicit header: RecordHeader, table: Table): Expression = {
    val columnName = header.getColumn(expr).getOrElse(throw IllegalArgumentException(
      expected = s"Expression in ${header.expressions.mkString("[", ", ", "]")}",
      actual = expr)
    )
    if(table.columns.contains(columnName)) {
      UnresolvedFieldReference(columnName)
    } else {
      null_lit(expr.cypherType.getFlinkType)
    }
  }

  implicit class CypherValueConversion(val v: CypherValue) extends AnyVal {

    def toFlinkLiteral: Expression = {
      v.cypherType.ensureFlinkCompatible()
      v match {
        case list: CypherList => {
          val listValues = list.value.map(_.toFlinkLiteral)
          array(listValues.head, listValues.tail: _*)
        }
        case map: CypherMap => MapConstructor(map.value.foldLeft(Seq.empty[Expression]) {
          case (acc, (key, value)) => {
            acc :+ expressions.Literal(key, Types.STRING)
            acc :+ value.toFlinkLiteral
          }
        })
        case _ => expressions.Literal(v.unwrap, v.cypherType.getFlinkType)
      }
    }
  }

}
