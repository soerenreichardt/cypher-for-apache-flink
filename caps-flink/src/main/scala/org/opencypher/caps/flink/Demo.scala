package org.opencypher.caps.flink

import org.apache.flink.streaming.api.scala._
import org.opencypher.caps.api.value.CypherValue.{CypherInteger, CypherMap, CypherString}

object Demo extends App {

  val session = CAPFSession.create

  val nodesDataSet = session.env.fromCollection(DemoData.nodes)
  val relsDataSet = session.env.fromCollection(DemoData.rels)

  session.readFrom(nodesDataSet, relsDataSet)

}

object DemoData {

  val nodes = Seq(
    Seq(0L, Set("Person"), CypherMap("name" -> CypherString("Alice"), "age" -> CypherInteger(42))),
    Seq(1L, Set("Person"), CypherMap("name" -> CypherString("Bob"), "age" -> CypherInteger(23))))
  val rels = Seq(2L, 0L, 1L, "KNOWS", CypherMap("since" -> CypherString("2018")))

}
