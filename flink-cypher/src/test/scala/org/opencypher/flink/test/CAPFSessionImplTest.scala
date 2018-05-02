package org.opencypher.flink.test

import org.opencypher.flink.CAPFRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.flink.test.fixture.TeamDataFixture

import scala.collection.Bag

class CAPFSessionImplTest extends CAPFTestSuite with TeamDataFixture {

  it("can execute sql on registred tables") {
    CAPFRecords.wrap(personDF).register("people")
    CAPFRecords.wrap(knowsDF).register("knows")

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
