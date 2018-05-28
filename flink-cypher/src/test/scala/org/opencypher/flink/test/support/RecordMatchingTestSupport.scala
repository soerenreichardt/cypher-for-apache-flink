package org.opencypher.flink.test.support

import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.apache.flink.table.api.scala._
import org.opencypher.flink.CAPFRecords
import org.opencypher.flink.CAPFRecordHeader._
import org.opencypher.flink.CAPFConverters._
import org.opencypher.flink.FlinkUtils._
import org.opencypher.flink.schema.EntityTable._
import org.opencypher.flink.test.CAPFTestSuite
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.{FieldSlotContent, OpaqueField, ProjectedExpr, RecordHeader}
import org.scalatest.Assertion

import scala.collection.immutable.{Bag, HashedBagConfiguration}

trait RecordMatchingTestSupport {

  self: CAPFTestSuite =>

  implicit val bagConfig: HashedBagConfiguration[CypherMap] = Bag.configuration.compact[CypherMap]

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

    def shouldMatchOpaquely(expectedRecords: CAPFRecords): Assertion = {
      RecordMatcher(projected(records)) shouldMatch projected(expectedRecords)
    }

    private def projected(records: CAPFRecords): CAPFRecords = {
      val newSlots = records.header.slots.map(_.content).map {
        case slot: FieldSlotContent => OpaqueField(slot.field)
        case slot: ProjectedExpr => OpaqueField(Var(slot.expr.withoutType)(slot.cypherType))
      }
      val newHeader = RecordHeader.from(newSlots: _*)
      val columnRenameExpressions = records.data.columns.zip(newHeader.internalHeader.columns).map {
        case (oldName, newName) => UnresolvedFieldReference(oldName) as Symbol(newName)
      }
      val newData = records.data.select(columnRenameExpressions: _*)
      CAPFRecords.verifyAndCreate(newHeader, newData)(records.capf)
    }
  }

  implicit class RichRecords(records: CypherRecords) {
    val capfRecords = records.asCapf

    def toMaps: Bag[CypherMap] = {
      val rows = capfRecords.toTable().collect().map { r =>
        val properties = capfRecords.header.slots.map { s =>
          s.content match {
            case f: FieldSlotContent => f.field.name -> r.getCypherValue(f.key, capfRecords.header)
            case x => x.key.withoutType -> r.getCypherValue(x.key, capfRecords.header)
          }
        }.toMap
        CypherMap(properties)
      }
      Bag(rows: _*)
    }

    def toMapsWithCollectedEntities: Bag[CypherMap] =
      Bag(capfRecords.toCypherMaps.collect(): _*)
  }
}