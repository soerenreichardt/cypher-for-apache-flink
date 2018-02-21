package org.opencypher.caps.flink

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.opencypher.caps.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.caps.flink.schema._


object Demo extends App {

  val session = CAPFSession.create

  private val nodeDataSet = session.env.fromCollection(DemoData.nodes)
  val relsDataSet = session.env.fromCollection(DemoData.rels)

  val nodes = session.tableEnv.fromDataSet(nodeDataSet, 'id, 'person, 'name, 'age)
  val rels = session.tableEnv.fromDataSet(relsDataSet, 'id, 'source, 'target, 'type, 'since)

  val nodeMapping = NodeMapping
    .withSourceIdKey("id")
    .withImpliedLabel("Person")
    .withPropertyKey("name")
    .withPropertyKey("age")

  val relMapping = RelationshipMapping
    .withSourceIdKey("id")
    .withSourceStartNodeKey("source")
    .withSourceEndNodeKey("target")
    .withRelType("KNOWS")
    .withPropertyKey("since")

  val nodeTable = CAPFNodeTable(nodeMapping, nodes)
  val relTable = CAPFRelationshipTable(relMapping, rels)

  session.readFrom(nodeTable, relTable)
}

object DemoData {
  val nodes = Seq(
    (0L, "Person", "Alice", 26),
    (1L, "Person", "Bob", 23)
  )

  val rels = Seq(
    (2L, 0L, 1L, "KNOWS", "2018")
  )

}
