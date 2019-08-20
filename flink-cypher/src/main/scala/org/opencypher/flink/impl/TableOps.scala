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

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils.DataSetUtils
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableSchema, Types}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.impl.convert.FlinkConversions._
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.okapi.impl.exception
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{Expr, Param}
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.impl.table.RecordHeader

object TableOps {

  implicit class CypherRow(r: Row) {

    def getCypherValue(expr: Expr, header: RecordHeader, columnNameToIndex: Map[String, Int])
      (implicit context: RelationalRuntimeContext[FlinkTable]): CypherValue = {
      expr match {
        case Param(name) => context.parameters(name)
        case _ =>
          header.getColumn(expr).headOption match {
            case None => throw IllegalArgumentException(s"slot for $expr")
            case Some(column) => CypherValue(r.getField(columnNameToIndex(column)))
          }
      }
    }
  }

  implicit class RichTableSchema(val schema: TableSchema) extends AnyVal {

    def columnNameToIndex: Map[String, Int] = schema.getFieldNames.zipWithIndex.toMap
  }

  implicit class RichTable(val table: Table) extends AnyVal {

    def cypherTypeForColumn(columnName: String): CypherType = {
      val compatibleCypherType = table.getSchema.getFieldType(columnName).get.cypherCompatibleDataType.flatMap(_.toCypherType())
      compatibleCypherType.getOrElse(
        throw IllegalArgumentException("a supported Flink Type that can be converted to CypherType", table.getSchema.getFieldType(columnName)))
    }

    def col(colName: String): Table =
      table.select(colName)

    def columns: Seq[String] = table.getSchema.getFieldNames

    def cross(other: Table)(implicit capf: CAPFSession): Table = {

      val crossedTableNames = table.columns.map(UnresolvedFieldReference) ++
        other.columns.map(UnresolvedFieldReference)
      val crossedTableTypes = table.getSchema.getFieldTypes.toSeq ++
        other.getSchema.getFieldTypes.toSeq

      val crossedDataSet = table.toDataSet[Row].cross(other).map { rowTuple =>
        rowTuple match {
          case (r1: Row, r2: Row) =>
            val r1Fields = Range(0, r1.getArity).map(r1.getField)
            val r2Fields = Range(0, r2.getArity).map(r2.getField)
            Row.of((r1Fields ++ r2Fields): _*)
        }
      }(Types.ROW(crossedTableTypes: _*), null)

      crossedDataSet.toTable(capf.tableEnv, crossedTableNames: _*)
    }

    def safeRenameColumn(oldName: String, newName: String): Table = {
      require(!table.columns.contains(newName),
        s"Cannot rename column `$oldName` to `$newName`. A column with name `$newName` exists already.")

      val renamedColumns = table.columns.map { col =>
          col match {
            case _ if col == oldName =>
              ResolvedFieldReference(oldName, table.getSchema.getFieldType(oldName).get) as Symbol(newName)
            case colName => ResolvedFieldReference(colName, table.getSchema.getFieldType(colName).get)
          }
      }

      table.select(renamedColumns: _*)
    }

    def safeRenameColumns(oldNames: Seq[String], newNames: Seq[String]): Table = {
      require(!newNames.forall(table.columns.contains),
        s"Cannot rename columns `$oldNames` to `$newNames`. One or more columns of `$newNames` exist already.")

      val namePairs = oldNames zip newNames
      val renames = namePairs.map(_ match {
        case (oldName: String, newName: String) => UnresolvedFieldReference(oldName) as Symbol(newName)
      })

      table.select(renames: _*)
    }

    def safeDropColumn(name: String): Table = {
      require(table.columns.contains(name),
        s"Cannot drop column `$name`. No column with that name exists.")
      val columnSelect = table.columns.filterNot(_ == (name))
      table.select(columnSelect.map(UnresolvedFieldReference): _*)
    }

    def safeDropColumns(names: String*): Table = {
      val nonExistentColumns = names.toSet -- table.columns
      require(nonExistentColumns.isEmpty,
        s"Cannot drop column(s) ${nonExistentColumns.map(c => s"`$c`").mkString(", ")}. They do not exist.")

      val dropColumnsToSelectExpression = table.columns.filter(!names.contains(_))
        .map(UnresolvedFieldReference(_))
      table.select(dropColumnsToSelectExpression: _*)
    }

    def safeJoin(other: Table, joinCols: Seq[(String, String)], joinType: String): Table = {
      require(joinCols.map(_._1).forall(col => !other.columns.contains(col)))
      require(joinCols.map(_._2).forall(col => !table.columns.contains(col)))

      val joinExpr = joinCols.map {
        case (l, r) => UnresolvedFieldReference(l) === UnresolvedFieldReference(r)
      }.foldLeft(Literal(true, BasicTypeInfo.BOOLEAN_TYPE_INFO): Expression)((acc, expr) => And(acc, expr))

      joinType match {
        case "inner" => table.join(other, joinExpr)
        case "left_outer" => table.leftOuterJoin(other, joinExpr)
        case "right_outer" => table.rightOuterJoin(other, joinExpr)
        case "full_outer" => table.fullOuterJoin(other, joinExpr)
        case x => throw exception.NotImplementedException(s"Join type $x")
      }
    }

    def safeAddColumn(name: String, expr: Expression): Table = {
      require(!table.columns.contains(name),
        s"Cannot add column `$name`. A column with that name exists already. " +
          s"Use `safeReplaceColumn` if you intend to replace that column.")
      table.select('*, expr as Symbol(name))
    }

    def safeAddColumns(columns: (String, Expression)*): Table = {
      columns.foldLeft(table) { case (tempTable, (colName, col)) =>
        tempTable.safeAddColumn(colName, col)
      }
    }

    def safeReplaceColumn(name: String, expr: Expression): Table = {
      require(table.columns.contains(name), s"Cannot replace column `$name`. No column with that name exists. " +
        s"Use `safeAddColumn` if you intend to add that column.")

      val fieldsWithExpressions = table.columns.map { field =>
        if (field == name) {
          expr as Symbol(name)
        } else {
          UnresolvedFieldReference(field)
        }
      }
      table.select(fieldsWithExpressions: _*)
    }

    def safeUpsertColumn(name: String, expr: Expression): Table = {
      if (table.columns.contains(name)) {
        safeReplaceColumn(name, expr)
      } else {
        safeAddColumn(name, expr)
      }
    }

    def safeToDataSet[T: TypeInformation](implicit capf: CAPFSession): DataSet[T] = {
      // TODO: preserve order of fields
      val nameToTypeMap = table.getSchema.getFieldNames.zip(table.getSchema.getFieldTypes).toMap

      val arrayTypes = nameToTypeMap.filter { pair => pair._2.isInstanceOf[BasicArrayTypeInfo[_, _]] }

      val mergeOp = new Merge(",")
      val expressions = nameToTypeMap.map { pair =>
        arrayTypes.map(_._1).toSet.contains(pair._1) match {
          case true => mergeOp(UnresolvedFieldReference(pair._1)).cast(Types.STRING) as Symbol(pair._1)
          case false => UnresolvedFieldReference(pair._1)
        }
      }.toSeq

      val sanitizedTable = table.select(expressions: _*)
      sanitizedTable.toDataSet[T]
    }

    def safeAddIdColumn(idColumnName: String)(implicit capf: CAPFSession): Table = {
      require(!table.getSchema.getFieldNames.toSet.contains(idColumnName),
      "Cannot create column `id` as it already exists")

      val tableTypes = table.getSchema.getFieldTypes
      val tableNames = table.getSchema.getFieldNames

      val dsWithIndex = table.toDataSet[Row].zipWithUniqueId

      val flattenedDs = dsWithIndex.map { rowWithIndex =>
        rowWithIndex match {
          case (l: Long, r: Row) =>
            val rowAsSeq = (0 until r.getArity).map(r.getField)
            Row.of(Seq(l.asInstanceOf[java.lang.Long]) ++ rowAsSeq: _*)
        }
      }(Types.ROW(Seq(Types.LONG) ++ tableTypes: _*), null)

      flattenedDs.safeCreateTableFromDataSet(
        Seq(idColumnName) ++ tableNames,
        Seq(Types.LONG) ++ tableTypes
      )
    }

    def withCypherCompatibleTypes: Table = {
      val castExprs = table.getSchema.getFieldNames.zip(table.getSchema.getFieldTypes).map {
        case (fieldName, fieldType) =>
          Seq(
            UnresolvedFieldReference(fieldName).cast(fieldType.cypherCompatibleDataType.getOrElse(
              throw IllegalArgumentException(
                s"a Flink type supported by Cypher: ${supportedTypes.mkString("[", ", ", "]")}",
                s"type $fieldType of field $fieldName"
              )
            )) as Symbol(fieldName))
      }.reduce(_ ++ _)

      table.select(castExprs: _*)
    }

  }

  implicit class RichDataSet(val ds: DataSet[Row]) extends AnyVal {

    def safeCreateTableFromDataSet(columnNames: Seq[String], rowTypes: Seq[TypeInformation[_]])
      (implicit capf: CAPFSession): Table = {

      implicit val rowTypeInfo: RowTypeInfo = new RowTypeInfo(rowTypes: _*)
      capf.tableEnv.fromDataSet(
        ds,
        columnNames.map(UnresolvedFieldReference): _*
      )
    }

  }

}

class Merge(token: String) extends ScalarFunction {

  def eval(str: Array[String]): String = {
    str.mkString(token)
  }

}

class BitshiftRight(numBits: Int) extends ScalarFunction {

  def eval(idField: Long): Long = {
    idField >> numBits
  }
}

class BitwiseAnd(other: Long) extends ScalarFunction {

  def eval(field: Long): Long = {
    field & other
  }
}

class BitwiseOr(field: Long) extends ScalarFunction {

  def eval(other: Long): Long = {
    field | other
  }
}
