package org.opencypher.flink

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.expressions._
import org.apache.flink.types.Row
import org.opencypher.flink.schema.EntityTable._

object TableOps {

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

      table.join(other, joinExpr)
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

  }

}
