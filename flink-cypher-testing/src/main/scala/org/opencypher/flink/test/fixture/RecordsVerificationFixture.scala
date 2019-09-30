package org.opencypher.flink.test.fixture

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.flink.test.CAPFTestSuite
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.testing.Bag._

trait RecordsVerificationFixture {

  self: CAPFTestSuite =>

  protected def verify(records: RelationalCypherRecords[FlinkTable], expectedExprs: Seq[Expr], expectedData: Bag[Row])
    (implicit session: CAPFSession): Unit = {
    val table = records.table
    val header = records.header
    val expectedColumns = expectedExprs.map(header.column)
    table.physicalColumns.length should equal(expectedColumns.size)
    table.physicalColumns.toSet should equal(expectedColumns.toSet)

    val actual = table.select(expectedColumns: _*).table.toDataSet[Row].collect().toBag

    actual should equal(expectedData)
  }
}
