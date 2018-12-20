package org.opencypher.flink.test

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.opencypher.flink.api.value.CAPFNode
import org.opencypher.flink.impl.CAPFRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.flink.test.fixture.{GraphConstructionFixture, TeamDataFixture}
import org.opencypher.okapi.api.graph.{Namespace, QualifiedGraphName}
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._

class CAPFSessionImplTest extends CAPFTestSuite with TeamDataFixture with GraphConstructionFixture {

  it("can use multiple session graph data sources") {
    capf.registerSource(Namespace("working"), new SessionGraphDataSource())
    capf.registerSource(Namespace("foo"), new SessionGraphDataSource())

    val g1 = initGraph("CREATE (:A)")
    val g2 = initGraph("CREATE (:B)")
    val g3 = initGraph("CREATE (:C)")

    capf.catalog.store(QualifiedGraphName("session.a"), g1)
    capf.catalog.store(QualifiedGraphName("working.a"), g2)
    capf.cypher("CREATE GRAPH working.b { FROM GRAPH working.a RETURN GRAPH }")
    capf.catalog.store(QualifiedGraphName("foo.bar.baz.a"), g3)

    val r1 = capf.cypher("FROM GRAPH a MATCH (n) RETURN n")
    val r2 = capf.cypher("FROM GRAPH working.a MATCH (n) RETURN n")
    val r3 = capf.cypher("FROM GRAPH wokring.b MATCH (n) RETURN n")
    val r4 = capf.cypher("FROM GRAPH foo.bar.baz.a MATCH (n) RETURN n")

    r1.records.collect.toBag should equal(Bag(
      CypherMap("n" -> CAPFNode(0L, Set("A")))
    ))
    r2.records.collect.toBag should equal(Bag(
      CypherMap("n" -> CAPFNode(0L, Set("B")))
    ))
    r3.records.collect.toBag should equal(Bag(
      CypherMap("n" -> CAPFNode(0L, Set("B")))
    ))
    r4.records.collect.toBag should equal(Bag(
      CypherMap("n" -> CAPFNode(0L, Set("C")))
    ))
  }

  it("can execute sql on registred tables") {

    tableEnv.registerTable("people", personDF)
    tableEnv.registerTable("knows", knowsDF)

    val sqlResult = capf.sql(
      """
        |SELECT people.name AS me, knows.since AS since p2.name AS you
        |FROM people
        |INNER JOIN knows ON knows.src = people.id
        |INNER JOIN people p2 ON knows.dst = p2.id
      """.stripMargin
    )

    sqlResult.collect.toBag should equal(Bag(
      CypherMap("me" -> "Mats", "since" -> 2017, "you" -> "Martin"),
      CypherMap("me" -> "Mats", "since" -> 2016, "you" -> "Max"),
      CypherMap("me" -> "Mats", "since" -> 2015, "you" -> "Stefan"),
      CypherMap("me" -> "Martin", "since" -> 2016, "you" -> "Max"),
      CypherMap("me" -> "Martin", "since" -> 2013, "you" -> "Stefan"),
      CypherMap("me" -> "Max", "since" -> 2016, "you" -> "Stefan")
    ))
  }

}
