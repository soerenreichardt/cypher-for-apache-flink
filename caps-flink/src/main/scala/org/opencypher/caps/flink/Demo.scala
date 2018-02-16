package org.opencypher.caps.flink

import org.apache.flink.streaming.api.scala._
import org.opencypher.caps.api.value.CypherValue.{CypherInteger, CypherMap, CypherString}
import org.opencypher.caps.flink.value.{CAPFNode, CAPFRelationship}

object Demo extends App {

  val session = CAPFSession.create

  val nodesDataSet = session.env.fromCollection(DemoData.nodes)
  val relsDataSet = session.env.fromCollection(DemoData.rels)

  val nodes = session.tableEnv.fromDataSet(nodesDataSet)
  val rels = session.tableEnv.fromDataSet(relsDataSet)

  session.readFrom(nodes, rels)

}

object DemoData {

  val nodes = Seq(
    CAPFNode(0L, Set("Person"), CypherMap("name" -> CypherString("Alice"), "age" -> CypherInteger(42))),
    CAPFNode(1L, Set("Person"), CypherMap("name" -> CypherString("Bob"), "age" -> CypherInteger(23)))
  )
  val rels = Seq(CAPFRelationship(2L, 0L, 1L, "KNOWS", CypherMap("since" -> CypherString("2018"))))

}
