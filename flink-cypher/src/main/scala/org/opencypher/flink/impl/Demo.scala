package org.opencypher.flink.impl

import java.sql._
import java.time.LocalDate

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.opencypher.flink.api.io.fs.FileBasedDataSource
import org.opencypher.flink.api.io.{CAPFNodeTable, CAPFRelationshipTable, CsvDataSource}
import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.okapi.api.configuration.Configuration.PrintTimings
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.impl.util.Measurement
import org.opencypher.okapi.ir.api.configuration.IrConfiguration.PrintIr
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintPhysicalPlan

object Demo extends App {

  val session = CAPFSession.local()

  val d = Date.valueOf(LocalDate.now())

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

  val nodeTable = CAPFNodeTable.fromMapping(nodeMapping, nodes)
  val relTable = CAPFRelationshipTable.fromMapping(relMapping, rels)

  val graph: CAPFGraph = session.readFrom(nodeTable, relTable)

  PrintIr.set()
  PrintLogicalPlan.set()
  PrintPhysicalPlan.set()
  PrintTimings.set()
  val planning = Measurement.time(graph.cypher("MATCH (n:Person)-[r:KNOWS]->(n2:Person) WHERE n.age >= 26 RETURN n.age AS age"))
  println("Planning: " + planning._2)
  val translation = Measurement.time(planning._1.getRecords.asCapf.table.toDataSet[Row])
  println("Translation: " + translation._2)
  val execution = Measurement.time(translation._1.collect())
  println("Execution: " +  execution._2)
  println("Flink execution: " + session.env.getLastJobExecutionResult.getNetRuntime)
  println(session.tableEnv.explain(planning._1.getRecords.asCapf.table))
//  graph.cypher("MATCH (n:Person)-[r:KNOWS*2..2]->(n2:Person) RETURN n.name, n2.name").getRecords.show                   // var expand
//  graph.cypher("MATCH (n:Person) WHERE (n)--({age: 29}) RETURN n.name").getRecords.show                               // exists
//  graph.cypher("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(b {age: 29}) RETURN n.name, b.name").getRecords.show   // optional match

//  graph.cypher("MATCH (n) RETURN CASE n.age WHEN 26 THEN 'Alice' WHEN 23 THEN 'Bob' ELSE 'other' END AS name").getRecords.show

//  graph.cypher(
//    """
//      |MATCH (a {name: 'Alice'}), (b {name: 'Bob'})
//      |MATCH (a)-[e]->(x)<-[f]-(b)
//      |RETURN x
//    """.stripMargin).getRecords.show

//  graph.cypher("MATCH (a)-->(b)-->(c) WHERE (a) <> (c) RETURN a.name, c.name").getRecords.show
//  graph.cypher("MATCH (n) RETURN n.name, n.age ORDER BY n.age SKIP 4 LIMIT 4").getRecords.show
//  graph.cypher(
//    """
//      |MATCH (a:Person)
//      |WITH a.age AS age
//      | LIMIT 1
//      |MATCH (b)
//      |WHERE b.age = age
//      |RETURN b
//    """.stripMargin).getRecords.show
//  graph.cypher("WITH 'foo' AS bar UNWIND [1, 2, 3] AS x RETURN x, bar").getRecords.show
//  graph.cypher("MATCH (n:Employee), (m: Person) RETURN (n)-[]->(m)").getRecords.show
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
    (5L, 1L, 3L, "KNOWS", "2010"),
    (6L, 0L, 3L, "KNOWS", "2001")
  )
}

object CsvDemo extends App {

  implicit val session: CAPFSession = CAPFSession.local()

  val csvFolder = getClass.getResource("/csv").getFile
  session.registerSource(Namespace("csv"), CsvDataSource(rootPath = csvFolder))

  val purchaseNetwork = session.catalog.graph("csv.products")

  session.cypher(
    """
      |FROM GRAPH csv.products
      |MATCH (c:Customer)
      |RETURN *
    """.stripMargin
  ).getRecords.show
}

object OrcDemo extends App {

  implicit val session: CAPFSession = CAPFSession.local()

  val orcFolder = getClass.getResource("/orc").getPath
  session.registerSource(Namespace("orc"), new FileBasedDataSource(orcFolder, "orc"))

  session.cypher(
    """
      |FROM GRAPH orc.sf1
      |MATCH (n:Person)
      |RETURN n.firstName
    """.stripMargin
  ).getRecords.show
}
