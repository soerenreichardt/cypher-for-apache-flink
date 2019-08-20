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
package org.opencypher.flink.test.support

import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.flink.impl.CAPFRecords
import org.opencypher.flink.test.CAPFTestSuite
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.scalatest.Assertion

trait RecordMatchingTestSupport {

  self: CAPFTestSuite =>

//  implicit class RichRow(r: Row) {
//
//    def getCypherValue(expr: Expr, header: RecordHeader)
//      (implicit context: RelationalRuntimeContext[FlinkTable]): CypherValue = {
//      expr match {
//        case Param(name) => context.parameters(name)
//        case _ =>
//          header.getColumn(expr) match {
//            case None => throw IllegalArgumentException(
//              expected = s"column for $expr",
//              actual = header.pretty)
//            case Some(column) => CypherValue(r.getField())
//          }
//      }
//    }
//  }

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

    def toMaps: Bag[CypherMap] = Bag(capfRecords.toCypherMaps.collect(): _*)
  }
}