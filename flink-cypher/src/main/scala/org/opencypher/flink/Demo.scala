package org.opencypher.flink

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.opencypher.flink.io.FileCsvPropertyGraphDataSource
import org.opencypher.flink.schema.{CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintPhysicalPlan


object Demo extends App {

  val session = CAPFSession.create()

  private val nodeDataSet = session.env.fromCollection(DemoData.nodes)
  val relsDataSet = session.env.fromCollection(DemoData.rels)

  val nodes = session.tableEnv.fromDataSet(nodeDataSet, 'ID, 'EMPLOYEE, 'NAME, 'AGE)
  val rels = session.tableEnv.fromDataSet(relsDataSet, 'ID, 'SOURCE, 'TARGET, 'TYPE, 'SINCE)

  val nodeMapping = NodeMapping
    .withSourceIdKey("ID")
    .withImpliedLabel("Person")
    .withOptionalLabel("Employee", "EMPLOYEE")
    .withPropertyKey("name", "NAME")
    .withPropertyKey("age", "AGE")

  val relMapping = RelationshipMapping
    .withSourceIdKey("ID")
    .withSourceStartNodeKey("SOURCE")
    .withSourceEndNodeKey("TARGET")
    .withSourceRelTypeKey("TYPE", Set("KNOWS"))
    .withPropertyKey("since", "SINCE")

  val nodeTable = CAPFNodeTable(nodeMapping, nodes)
  val relTable = CAPFRelationshipTable(relMapping, rels)

  val graph: CAPFGraph = session.readFrom(nodeTable, relTable)

  PrintPhysicalPlan.set()
//  graph.cypher("MATCH (n:Person)-[r:KNOWS]->(n2:Person) RETURN *").getRecords.show
  graph.cypher("MATCH (n:Person)-[r:KNOWS*1..3]->(n2:Person) RETURN *").getRecords.show
//  graph.cypher(
//    """
//      |MATCH (a {name: 'Alice'}), (b {name: 'Bob'})
//      |MATCH (a)-[e]->(x)<-[f]-(b)
//      |RETURN x
//    """.stripMargin).getRecords.show

//  graph.cypher("MATCH (n) RETURN n.name, n.age ORDER BY n.age").getRecords.show
//  graph.cypher("WITH 'foo' AS bar UNWIND ['1', '2', '3'] AS x RETURN x, bar").getRecords.show
//  graph.cypher("MATCH (n:Employee) RETURN n").getRecords.show

}

object DemoData {
  val nodes = Seq(
    (0L, false, "Alice", 26),
    (1L, false, "Bob", 23),
    (3L, true, "Pete", 29)
  )

  val rels = Seq(
    (2L, 0L, 1L, "KNOWS", "2018"),
    (4L, 3L, 1L, "KNOWS", "2010"),
    (5L, 1L, 3L, "KNOWS", "2010")
  )

}

object CsvDemo extends App {

  private val resourcesPath = getClass.getResource("/csv").getPath.substring(1)
  implicit val session = CAPFSession.create()

  val dataSource = new FileCsvPropertyGraphDataSource(rootPath = resourcesPath)
  val graph = dataSource.graph(GraphName("sn"))

}
