package org.opencypher.flink

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.opencypher.flink.schema.EntityTable._
import org.opencypher.okapi.impl.exception
import org.opencypher.flink.Tags._

object TableOps {

  implicit class ColumnTagging(val col: UnresolvedFieldReference) extends AnyVal {

    def replaceTag(from: Int, to: Int): UnresolvedFieldReference = {

    }

    def setTag(tag: Int): UnresolvedFieldReference = {
      val tagLit = Literal(tag.toLong << idBits, Types.LONG)
      val newId = col
        .
    }

    def getTag: Expression = {
      val shiftRight = new BitshiftRight(idBits)
      shiftRight(col)
    }
  }

  implicit class RichTable(val table: Table) extends AnyVal {

    def col(colName: String): Table =
      table.select(colName)

    def cross(other: Table)(implicit capf: CAPFSession): Table = {

      val crossedTableNames = table.columns.map(UnresolvedFieldReference) ++
        other.columns.map(UnresolvedFieldReference)
      val crossedTableTypes = table.getSchema.getTypes.toSeq ++
        other.getSchema.getTypes.toSeq

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
              Symbol(oldName) as Symbol(newName)
            case _ => Symbol(col) as Symbol(col)
          }
      }

      table.select(renamedColumns: _*)
    }

    def safeRenameColumns(oldNames: Seq[String], newNames: Seq[String]): Table = {
      require(!newNames.forall(table.columns.contains),
        s"Cannot rename columns `$oldNames` to `$newNames`. One or more columns of `$newNames` exist already.")

      val namePairs = oldNames zip newNames
      val renames = namePairs.map(_ match {
        case (oldName: String, newName: String) => Symbol(oldName) as Symbol(newName)
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
        case "left" => table.leftOuterJoin(other, joinExpr)
        case "right" => table.rightOuterJoin(other, joinExpr)
        case "outer" | "full" => table.fullOuterJoin(other, joinExpr)
        case x => throw exception.NotImplementedException(s"Join type $x")
      }
    }

    def safeAddColumn(name: String, col: Table): Table = {
      require(!table.columns.contains(name),
        s"Cannot add column `$name`. A column with that name exists already. " +
      s"Use `safeReplaceColumn` if you intend to replace that column.")
      table.select('*, col.columns.head as Symbol(name))
    }

    def safeAddColumn(name: String, expr: Expression): Table = {
      require(!table.columns.contains(name),
        s"Cannot add column `$name`. A column with that name exists already. " +
          s"Use `safeReplaceColumn` if you intend to replace that column.")
      table.select('*, expr as Symbol(name))
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

    def safeToDataSet[T: TypeInformation](implicit capf: CAPFSession): DataSet[T] = {
      // TODO: preserve order of fields
      val nameToTypeMap = table.getSchema.getColumnNames.zip(table.getSchema.getTypes).toMap

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

    def safeReplaceTags(columnName: String, replacements: Map[Int, Int]): Table = {
      val dataType = table.getSchema.getType(columnName)
      require(dataType == Types.LONG, s"Cannot remap long values in Column with type $dataType")

      val col = UnresolvedFieldReference(columnName)
      val updatedCol = replacements.foldLeft(col) {
        case (current, (from, to)) => current.relaceTag(from, to)
      }

      safeReplaceColumn(columnName, updatedCol)
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

class BitwiseAnd(other: AnyRef) extends ScalarFunction {

  def eval(field: AnyRef): AnyRef = {
    field & other
  }
}
