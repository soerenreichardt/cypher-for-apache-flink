package org.opencypher.flink.test.support

import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.opencypher.flink.test.CAPFTestSuite
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.flink.impl.CAPFRecords
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.scalatest.Assertion

import scala.collection.JavaConverters._

trait RecordMatchingTestSupport {

  self: CAPFTestSuite =>

  implicit class RecordMatcher(records: CAPFRecords) {
    def shouldMatch(expected: CypherMap*): Assertion = {
      records.collect.toBag should equal(Bag(expected: _*))
    }

    def shouldMatch(expectedRecords: CAPFRecords): Assertion = {
      records.header should equal(expectedRecords.header)

      val actualData = records.toLocalIterator.toSet
      val expectedData = expectedRecords.toLocalIterator.toSet
      actualData should equal(expectedData)
    }

  }

  implicit class RichRecords(records: CypherRecords) {
    val capfRecords: CAPFRecords = records.asCapf

    def toMaps: Bag[CypherMap] = {
      val rows = capfRecords.flinkTable.collect().map { r =>
        val properties = capfRecords.header.expressions.map {
          case v: Var => v.name -> r.getCypherValue(v, capfRecords.header, capfRecords.flinkTable.getSchema.columnNameToIndex)
          case e => e.withoutType -> r.getCypherValue(e, capfRecords.header, capfRecords.flinkTable.getSchema.columnNameToIndex)
        }.toMap
        CypherMap(properties)
      }
      Bag(rows: _*)
    }

    def toMapsWithCollectedEntities: Bag[CypherMap] =
      Bag(capfRecords.toCypherMaps.collect(): _*)
  }
}