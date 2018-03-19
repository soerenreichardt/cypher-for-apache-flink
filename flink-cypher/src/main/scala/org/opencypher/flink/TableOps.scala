package org.opencypher.flink

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.expressions._
import org.opencypher.flink.schema.EntityTable._

object TableOps {

  implicit class RichTable(val table: Table) extends AnyVal {

    def col(colName: String): Table =
      table.select(colName)

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

  }

}
