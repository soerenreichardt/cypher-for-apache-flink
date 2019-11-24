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
package org.apache.flink.impl.acceptance

import org.opencypher.flink.test.CAPFTestSuite
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.temporal.Duration
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._

class AggregationTests extends CAPFTestSuite with ScanGraphInit {

  describe("AVG") {

    it("avg(prop) with integers in WITH") {
      val graph = initGraph("CREATE ({val:2}),({val:4}),({val:6})")

      val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

      result.records.collect.toBag should equal(Bag(
        CypherMap("res" -> 4)
      ))
    }

    it("avg(prop) with integers in RETURN") {
      val graph = initGraph("CREATE ({val: 2}),({val: 4}),({val: 6})")

      val result = graph.cypher("MATCH (n) RETURN AVG(n.val) AS res")

      result.records.collect.toBag should equal(Bag(
        CypherMap("res" -> 4)
      ))
    }

    it("avg(prop) with integers in RETURN without alias") {
      val graph = initGraph("CREATE ({val: 2}),({val: 4}),({val: 6})")

      val result = graph.cypher("MATCH (n) RETURN AVG(n.val)")

      result.records.toMaps should equal(Bag(
        CypherMap("AVG(n.val)" -> 4)
      ))
    }

    it("avg(prop) with floats in WITH") {
      val graph = initGraph("CREATE ({val:5.0D}),({val:5.0D}),({val:0.5D})")

      val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 3.5)
      ))
    }

    it("avg(prop) with floats in RETURN") {
      val graph = initGraph("CREATE ({val:5.0D}),({val:5.0D}),({val:0.5D})")

      val result = graph.cypher("MATCH (n) RETURN AVG(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 3.5)
      ))
    }

    it("avg(prop) with single null value in WITH") {
      val graph = initGraph("CREATE ({val:42.0D}),({val:23.0D}),()")

      val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 32.5)
      ))
    }

    it("avg(prop) with single null value in RETURN") {
      val graph = initGraph("CREATE ({val:42.0D}),({val:23.0D}),()")

      val result = graph.cypher("MATCH (n) RETURN AVG(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 32.5)
      ))
    }

    it("avg(prop) with only null values in WITH") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) WITH AVG(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    it("avg(prop) with only null values in RETURN") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) RETURN AVG(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    ignore("avg on durations") {
      val result = capf.graphs.empty.cypher("UNWIND [duration('P1DT12H'), duration('P1DT20H')] AS d RETURN AVG(d) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> Duration(days = 1, hours = 16))
      ))
    }
  }

  describe("COUNT") {

    it("count(*) in WITH") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) WITH count(*) AS nbrRows RETURN nbrRows")

      result.records.toMaps should equal(Bag(
        CypherMap("nbrRows" -> 6)
      ))
    }

    it("count(*) in RETURN") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) RETURN count(*) AS nbrRows")

      result.records.toMaps should equal(Bag(
        CypherMap("nbrRows" -> 6)
      ))
    }

    it("count(n) in RETURN") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) RETURN count(n) AS nbrRows")

      result.records.toMaps should equal(Bag(
        CypherMap("nbrRows" -> 6)
      ))
    }

    it("count(n) in RETURN without alias") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) RETURN count(n)")

      result.records.toMaps should equal(Bag(
        CypherMap("count(n)" -> 6)
      ))
    }

    it("count(*) in return without alias") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) RETURN count(*)")

      result.records.toMaps should equal(Bag(
        CypherMap("count(*)" -> 6)
      ))
    }

    it("simple count(prop)") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) WITH count(n.name) AS nonNullNames RETURN nonNullNames")

      result.records.toMaps should equal(Bag(
        CypherMap("nonNullNames" -> 3)
      ))
    }

    it("simple count(node)") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'bar'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) WITH count(n) AS nodes RETURN nodes")

      result.records.toMaps should equal(Bag(
        CypherMap("nodes" -> 6)
      ))
    }

    it("count after expand") {
      val graph = initGraph("CREATE ({name: 'foo'})-[:A]->(:B), ({name: 'bar'}), (), ()-[:A]->(:B), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n)-->(b:B) WITH count(b) AS nodes RETURN nodes")

      result.records.toMaps should equal(Bag(
        CypherMap("nodes" -> 2)
      ))
    }

    it("count() with grouping in RETURN clause") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'foo'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) RETURN n.name AS name, count(*) AS amount")

      result.records.toMaps should equal(Bag(
        CypherMap("name" -> "foo", "amount" -> 2),
        CypherMap("name" -> null, "amount" -> 3),
        CypherMap("name" -> "baz", "amount" -> 1)
      ))
    }

    it("count() with grouping in WITH clause") {
      val graph = initGraph("CREATE ({name: 'foo'}), ({name: 'foo'}), (), (), (), ({name: 'baz'})")

      val result = graph.cypher("MATCH (n) WITH n.name AS name, count(*) AS amount RETURN name, amount")

      result.records.toMaps should equal(Bag(
        CypherMap("name" -> "foo", "amount" -> 2),
        CypherMap("name" -> null, "amount" -> 3),
        CypherMap("name" -> "baz", "amount" -> 1)
      ))
    }

    it("count() with grouping on multiple keys") {
      val graph = initGraph("CREATE ({name: 'foo', age: 42}), ({name: 'foo', age: 42}), ({name: 'foo', age: 23}), (), (), ({name: 'baz', age: 23})")

      val result = graph
        .cypher("MATCH (n) WITH n.name AS name, n.age AS age, count(*) AS amount RETURN name, age, amount")

      result.records.toMaps should equal(Bag(
        CypherMap("name" -> "foo", "age" -> 23, "amount" -> 1),
        CypherMap("name" -> "foo", "age" -> 42, "amount" -> 2),
        CypherMap("name" -> "baz", "age" -> 23, "amount" -> 1),
        CypherMap("name" -> null, "age" -> null, "amount" -> 2)
      ))
    }

    ignore("counts distinct with grouping") {
      val graph = initGraph(
        """
          |CREATE (a:Start{id: 1})
          |CREATE (a)-[:REL]->({val: "foo"})
          |CREATE (a)-[:REL]->({val: "foo"})
        """.stripMargin)

      val result = graph.cypher(
        """
          |MATCH (a:Start)-->(b)
          |
          |RETURN a.id,
          |       count(distinct b.val) AS val
        """.stripMargin)

      result.records.toMaps should equal(Bag(
        CypherMap("a.id" -> 1, "val" -> 1)
      ))
    }
  }

  describe("MIN") {

    it("min(prop) in WITH") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),({val: 84})")

      val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 23L)
      ))
    }

    it("min(prop) in RETURN") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),({val: 84})")

      val result = graph.cypher("MATCH (n) RETURN MIN(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 23L)
      ))
    }

    it("min(prop) with single null value in WITH") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),()")

      val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 23L)
      ))
    }

    it("min(prop) with single null value in RETURN") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),()")

      val result = graph.cypher("MATCH (n) RETURN MIN(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 23L)
      ))
    }

    it("min(prop) with single null value in RETURN without alias") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),()")

      val result = graph.cypher("MATCH (n) RETURN MIN(n.val)")

      result.records.toMaps should equal(Bag(
        CypherMap("MIN(n.val)" -> 23L)
      ))
    }

    it("min(prop) with only null values in WITH") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) WITH MIN(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    it("min(prop) with only null values in RETURN") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) RETURN MIN(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    ignore("min on dates") {
      val result = capf.graphs.empty.cypher("UNWIND [date('2018-01-01'), date('2019-01-01')] AS d RETURN MIN(d) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> java.time.LocalDate.parse("2018-01-01"))
      ))
    }

    ignore("min on datetimes") {
      val result = capf.graphs.empty.cypher("UNWIND [localdatetime('2010-10-10T12:00'), localdatetime('2010-10-10T12:01')] AS d RETURN MIN(d) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> java.time.LocalDateTime.parse("2010-10-10T12:00"))
      ))
    }

    ignore("min on durations") {
      val result = capf.graphs.empty.cypher("UNWIND [duration('P1DT12H'), duration('P1DT200H')] AS d RETURN MIN(d) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> Duration(days = 1, hours = 12))
      ))
    }

    ignore("min on combination of temporal types") {
      val result = capf.graphs.empty.cypher("UNWIND [date('2018-01-01'), localdatetime('2010-10-10T12:01')] AS d RETURN MIN(d) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> java.time.LocalDateTime.parse("2010-10-10T12:01"))
      ))
    }
  }

  describe("MAX") {

    it("max(prop) in WITH") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),({val: 84})")

      val result = graph.cypher("MATCH (n) WITH MAX(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 84L)
      ))
    }

    it("max(prop) in RETURN") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),({val: 84})")

      val result = graph.cypher("MATCH (n) RETURN MAX(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 84L)
      ))
    }

    it("max(prop) with single null value in WITH") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),()")

      val result = graph.cypher("MATCH (n) WITH MAX(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 42L)
      ))
    }

    it("max(prop) with single null value in RETURN") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),()")

      val result = graph.cypher("MATCH (n) RETURN MAX(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 42L)
      ))
    }

    it("max(prop) with single null value in RETURN without alias") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),()")

      val result = graph.cypher("MATCH (n) RETURN MAX(n.val)")

      result.records.toMaps should equal(Bag(
        CypherMap("MAX(n.val)" -> 42L)
      ))
    }

    it("simple max(prop) with only null values in WITH") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) WITH MAX(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    it("simple max(prop) with only null values in RETURN") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) RETURN MAX(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    ignore("max on dates") {
      val result = capf.graphs.empty.cypher("UNWIND [date('2018-01-01'), date('2019-01-01')] AS d RETURN MAX(d) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> java.time.LocalDate.parse("2019-01-01"))
      ))
    }

    ignore("max on datetimes") {
      val result = capf.graphs.empty.cypher("UNWIND [localdatetime('2010-10-10T12:00'), localdatetime('2010-10-10T12:01')] AS d RETURN MAX(d) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> java.time.LocalDateTime.parse("2010-10-10T12:01"))
      ))
    }

    ignore("max on durations") {
      val result = capf.graphs.empty.cypher("UNWIND [duration('P10DT12H'), duration('P1DT24H')] AS d RETURN MAX(d) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> Duration(days = 10, hours = 12))
      ))
    }

    ignore("max on combination of temporal types") {
      val result = capf.graphs.empty.cypher("UNWIND [date('2018-01-01'), localdatetime('2010-10-10T12:01')] AS d RETURN MAX(d) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> java.time.LocalDateTime.parse("2018-01-01T00:00"))
      ))
    }

  }


  describe("SUM") {

    it("sum(prop) with integers in WITH") {
      val graph = initGraph("CREATE ({val: 2}),({val: 4}),({val: 6})")

      val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 12)
      ))
    }

    it("sum(prop) with integers in RETURN") {
      val graph = initGraph("CREATE ({val: 2}),({val: 4}),({val: 6})")

      val result = graph.cypher("MATCH (n) RETURN SUM(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 12)
      ))
    }

    it("sum(prop) with floats in WITH") {
      val graph = initGraph("CREATE ({val:5.0D}),({val:5.0D}),({val:0.5D})")

      val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 10.5)
      ))
    }

    it("sum(prop) with floats in RETURN") {
      val graph = initGraph("CREATE ({val:5.0D}),({val:5.0D}),({val:0.5D})")

      val result = graph.cypher("MATCH (n) RETURN SUM(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 10.5)
      ))
    }

    it("sum(prop) with floats in RETURN without alias") {
      val graph = initGraph("CREATE ({val:5.0D}),({val:5.0D}),({val:0.5D})")

      val result = graph.cypher("MATCH (n) RETURN SUM(n.val)")

      result.records.toMaps should equal(Bag(
        CypherMap("SUM(n.val)" -> 10.5)
      ))
    }

    it("simple sum(prop) with single null value in WITH") {
      val graph = initGraph("CREATE ({val:42.0D}),({val:23.0D}),()")

      val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 65.0)
      ))
    }

    it("simple sum(prop) with single null value in RETURN") {
      val graph = initGraph("CREATE ({val:42.0D}),({val:23.0D}),()")

      val result = graph.cypher("MATCH (n) RETURN SUM(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 65.0)
      ))
    }

    it("simple sum(prop) with only null values in WITH") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) WITH SUM(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    it("simple sum(prop) with only null values in RETURN") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) RETURN SUM(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    ignore("sum on durations") {
      val result = capf.graphs.empty.cypher("UNWIND [duration('P1DT12H'), duration('P1DT24H')] AS d RETURN SUM(d) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> Duration(days = 3, hours = 12))
      ))
    }
  }

  describe("stDev") {
    it("stDev on floats") {
      val result = capf.graphs.empty.cypher("UNWIND [98.17, 112.3, 102.6, 94.3, 108.1] AS numbers RETURN round(stDev(numbers)*1000)/1000.0 AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 7.274)
      ))
    }

    it("stDev on nullable list of floats") {
      val result = capf.graphs.empty.cypher("UNWIND [98.17, null, 102.6, 94.3, 108.1] AS numbers RETURN round(stDev(numbers)*1000)/1000.0 AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 5.936)
      ))
    }

    it("stDev on null") {
      val result = capf.graphs.empty.cypher("RETURN  stDev(null) AS res")

      // TODO: Spark returns null for stDev(null) instead of 0
      result.records.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }
  }

  describe("stDevP") {
    it("stDevP on floats") {
      val result = capf.graphs.empty.cypher("UNWIND [98.17, 112.3, 102.6, 94.3, 108.1] AS numbers RETURN round(stDevP(numbers)*1000)/1000.0 AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 6.506)
      ))
    }

    it("stDevP on nullable list of floats") {
      val result = capf.graphs.empty.cypher("UNWIND [98.17, null, 102.6, 94.3, 108.1] AS numbers RETURN round(stDevP(numbers)*1000)/1000.0 AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 5.140)
      ))
    }

    it("stDevP on null") {
      val result = capf.graphs.empty.cypher("RETURN  stDevP(null) AS res")

      // TODO: Spark returns null for stDevP(null) instead of 0
      result.records.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }
  }

  describe("percentileCont") {
    it("percentileContil on integers"){
      val result = capf.graphs.empty.cypher("UNWIND [1,2] AS values RETURN percentileCont(values, 0.5) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 1.5)
      ))
    }

    it("percentileContil with 1.0 as percentile"){
      val result = capf.graphs.empty.cypher("UNWIND [2,10,5,6] AS values RETURN percentileCont(values, 1.0) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 10.0)
      ))
    }

    it("percentileContil with 0.0 as percentile"){
      val result = capf.graphs.empty.cypher("UNWIND [2,10,5,6] AS values RETURN percentileCont(values, 0.0) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 2.0)
      ))
    }

    it("percentileContil on floats with null"){
      val result = capf.graphs.empty.cypher("UNWIND [10.0,null,2.0,6.0] AS values RETURN round(percentileCont(values, 0.62) * 1000) / 1000.0 AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 6.96)
      ))
    }

    it("percentileContil on floats"){
      val result = capf.graphs.empty.cypher("UNWIND [10.0,5.0,2.0,6.0] AS values RETURN percentileCont(values, 0.6) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 5.8)
      ))
    }
  }

  describe("percentileDisc") {
    it("percentileDisc on integers"){
      val result = capf.graphs.empty.cypher("UNWIND [10,5,2,6] AS values RETURN percentileDisc(values, 0.5) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 5)
      ))
    }

    it("percentileDisc with 1.0 as percentile"){
      val result = capf.graphs.empty.cypher("UNWIND [10.0,5.0,2.0,6.0] AS values RETURN percentileDisc(values, 1.0) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 10.0)
      ))
    }

    it("percentileDisc with 0.0 as percentile"){
      val result = capf.graphs.empty.cypher("UNWIND [10.0,5.0,2.0,6.0] AS values RETURN percentileDisc(values, 0.0) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 2.0)
      ))
    }

    it("percentileDisc on floats with null"){
      val result = capf.graphs.empty.cypher("UNWIND [10.0,null,2.0,6.0] AS values RETURN percentileDisc(values, 0.6) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 6.0)
      ))
    }

    it("percentileDisc on floats"){
      val graph = initGraph("CREATE ({age: 10.0}), ({age: 2.0}), ({age: 5.0}), ({age: 6.0})")
      val result = graph.cypher("MATCH (n) RETURN percentileDisc(n.age, 0.5) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> 5.0)
      ))
    }
  }

  describe("COLLECT") {

    it("collect(prop) with integers in WITH") {
      val graph = initGraph("CREATE ({val: 2}),({val: 4}),({val: 6})")

      val result = graph.cypher("MATCH (n) WITH COLLECT(n.val) AS res RETURN res")

      val rows = result.records.collect
      rows.length shouldBe 1
      val value1 = rows.head("res")
      value1.cast[CypherList].unwrap.toBag should equal(Bag(2, 4, 6))
    }

    it("collect(prop) with integers in RETURN") {
      val graph = initGraph("CREATE ({val: 2}),({val: 4}),({val: 6})")

      val result = graph.cypher("MATCH (n) RETURN COLLECT(n.val) AS res")

      val rows = result.records.collect
      rows.length shouldBe 1
      rows.head("res").cast[CypherList].unwrap.toBag should equal(Bag(2, 4, 6))
    }

    it("simple collect(prop) with single null value in WITH") {
      val graph = initGraph("CREATE ({val:42.0D}),({val:23.0D}),()")

      val result = graph.cypher("MATCH (n) WITH COLLECT(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> Seq(23.0, 42.0))
      ))
    }

    it("simple collect(prop) with single null value in RETURN") {
      val graph = initGraph("CREATE ({val:42.0D}),({val:23.0D}),()")

      val result = graph.cypher("MATCH (n) RETURN COLLECT(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> Seq(23.0, 42.0))
      ))
    }

    it("simple collect(prop) with only null values in WITH") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) WITH Collect(n.val) AS res RETURN res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> Seq.empty)
      ))
    }

    it("simple collect(prop) with only null values in RETURN") {
      val graph = initGraph("CREATE ({val:NULL}),(),()")

      val result = graph.cypher("MATCH (n) RETURN COLLECT(n.val) AS res")

      result.records.toMaps should equal(Bag(
        CypherMap("res" -> Seq.empty)
      ))
    }

    it("collects distinct lists with grouping") {
      val graph = initGraph(
        """
          |CREATE (a:Start{id: 1})
          |CREATE (a)-[:REL]->({val: "foo"})
          |CREATE (a)-[:REL]->({val: "foo"})
        """.stripMargin)

      val result = graph.cypher(
        """
          |MATCH (a:Start)-->(b)
          |
          |RETURN a.id,
          |       collect(distinct b.val) AS val
        """.stripMargin)

      result.records.toMaps should equal(Bag(
        CypherMap("a.id" -> 1, "val" -> CypherList("foo"))
      ))
    }
  }

  it("collects non-nullable strings that are not present on every match") {
    val graph = initGraph(
      """
        |CREATE (a:Person{id: 1, name:'Anna'})
        |CREATE (b:Person{id: 2, name:'Bob'})
        |CREATE (p1:Purchase{id: 3})
        |CREATE (a)-[:BOUGHT]->(p1)
      """.stripMargin)

    graph.cypher(
      """|MATCH (person:Person)-[:FRIEND_OF]-(friend:Person),
         |(friend)-[:IS]->(customer:Customer),
         |(customer)-[:BOUGHT]->(product:Product)
         |RETURN person.name AS for, collect(DISTINCT product.title) AS recommendations""".stripMargin)
  }

  describe("Combinations") {

    it("multiple aggregates in WITH") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),({val: 84})")

      val result = graph.cypher(
        """MATCH (n)
          |WITH
          | AVG(n.val) AS avg,
          | COUNT(*) AS cnt,
          | MIN(n.val) AS min,
          | MAX(n.val) AS max,
          | SUM(n.val) AS sum,
          | COLLECT(n.val) AS col
          |RETURN avg, cnt, min, max, sum, col""".stripMargin)

      val rows = result.records.collect
      rows.length shouldBe 1
      rows.head("col").cast[CypherList].unwrap.toBag should equal(Bag(23, 42, 84))
      rows.head("avg") should equal(CypherFloat(49.666666666666664))
      rows.head("cnt") should equal(CypherInteger(3))
      rows.head("min") should equal(CypherInteger(23))
      rows.head("max") should equal(CypherInteger(84))
      rows.head("sum") should equal(CypherInteger(149))
    }

    it("multiple aggregates in RETURN") {
      val graph = initGraph("CREATE ({val: 42}),({val: 23}),({val: 84})")

      val result = graph.cypher(
        """MATCH (n)
          |RETURN
          | AVG(n.val) AS avg,
          | COUNT(*) AS cnt,
          | MIN(n.val) AS min,
          | MAX(n.val) AS max,
          | SUM(n.val) AS sum,
          | COLLECT(n.val) AS col""".stripMargin)

      val rows = result.records.collect
      rows.length shouldBe 1
      rows.head("col").cast[CypherList].unwrap.toBag should equal(Bag(23, 42, 84))
      rows.head("avg") should equal(CypherFloat(49.666666666666664))
      rows.head("cnt") should equal(CypherInteger(3))
      rows.head("min") should equal(CypherInteger(23))
      rows.head("max") should equal(CypherInteger(84))
      rows.head("sum") should equal(CypherInteger(149))
    }

    it("computes multiple aggregates with grouping in RETURN clause") {
      val graph = initGraph("CREATE ({key: 'a', val: 42.0}),({key: 'a',val: 23.0}),({key: 'b', val: 84.0})")

      val result = graph.cypher(
        """MATCH (n)
          |RETURN
          | n.key AS key,
          | AVG(n.val) AS avg,
          | COUNT(*) AS cnt,
          | MIN(n.val) AS min,
          | MAX(n.val) AS max,
          | SUM(n.val) AS sum,
          | COLLECT(n.val) AS col""".stripMargin)

      result.records.toMaps should equal(Bag(
        CypherMap("key" -> "b", "avg" -> 84.0, "cnt" -> 1, "min" -> 84.0, "max" -> 84.0, "sum" -> 84.0, "col" -> Seq(84.0)),
        CypherMap(
          "key" -> "a", "avg" -> 32.5, "cnt" -> 2, "min" -> 23.0, "max" -> 42.0, "sum" -> 65.0, "col" -> Seq(23.0, 42.0))
      ))
    }

    it("computes multiple aggregates with grouping in WITH clause") {
      val graph = initGraph("CREATE ({key: 'a', val: 42}),({key: 'a',val: 23}),({key: 'b', val: 84})")

      val result = graph.cypher(
        """MATCH (n)
          |WITH
          | n.key AS key,
          | AVG(n.val) AS avg,
          | COUNT(*) AS cnt,
          | MIN(n.val) AS min,
          | MAX(n.val) AS max,
          | SUM(n.val) AS sum,
          | COLLECT(n.val) AS col
          |RETURN key, avg, cnt, min, max, sum, col""".stripMargin)

      result.records.toMaps should equal(Bag(
        CypherMap(
          "key" -> "a", "avg" -> 32.5, "cnt" -> 2, "min" -> 23L, "max" -> 42L, "sum" -> 65, "col" -> Seq(23, 42)),
        CypherMap("key" -> "b", "avg" -> 84.0, "cnt" -> 1, "min" -> 84, "max" -> 84, "sum" -> 84, "col" -> Seq(84))
      ))
    }
  }
}
