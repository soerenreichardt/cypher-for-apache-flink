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
package org.opencypher.flink.impl

import java.net.URI

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.opencypher.flink.api.{CAPFSession, GraphSources}
import org.opencypher.flink.api.io.CAPFElementTable
import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.okapi.api.configuration.Configuration.PrintTimings
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.impl.util.Measurement
import org.opencypher.okapi.ir.api.configuration.IrConfiguration.PrintIr
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintRelationalPlan

object Demo extends App {

  implicit val session = CAPFSession.local()

  private val nodeDataSet = session.env.fromCollection(DemoData.nodes)
  val relsDataSet = session.env.fromCollection(DemoData.rels)

  val nodes = session.tableEnv.fromDataSet(nodeDataSet, 'ID, 'EMPLOYEE, 'NAME, 'AGE)
  val rels = session.tableEnv.fromDataSet(relsDataSet, 'ID, 'SOURCE, 'TARGET, 'TYPE, 'SINCE)

  val nodeMapping = NodeMappingBuilder
    .withSourceIdKey("ID")
    .withImpliedLabel("Person")
    .withPropertyKey("name", "NAME")
    .withPropertyKey("age", "AGE")
    .build

  val relMapping = RelationshipMappingBuilder
    .withSourceIdKey("ID")
    .withSourceStartNodeKey("SOURCE")
    .withSourceEndNodeKey("TARGET")
    .withRelType("KNOWS")
    .withPropertyKey("since", "SINCE")
    .build

  val nodeTable = CAPFElementTable.create(nodeMapping, nodes)
  val relTable = CAPFElementTable.create(relMapping, rels)

  val graph = session.readFrom(nodeTable, relTable)

  PrintIr.set()
  PrintLogicalPlan.set()
  PrintRelationalPlan.set()
  PrintTimings.set()
//  graph.cypher("MATCH (n:Person) RETURN n.name").show
//  val planning = Measurement.time(graph.cypher("MATCH (n:Person)-[r:KNOWS]->(n2:Person) WHERE n.age >= 26 RETURN n.age AS age"))
//  println("Planning: " + planning._2)
//  val translation = Measurement.time(planning._1.getRecords.asCapf.table.toDataSet[Row])
//  println("Translation: " + translation._2)
//  val execution = Measurement.time(translation._1.collect())
//  println("Execution: " +  execution._2)
//  println("Flink execution: " + session.env.getLastJobExecutionResult.getNetRuntime)
//  println(session.tableEnv.explain(planning._1.getRecords.asCapf.table))
//  graph.cypher("MATCH (n:Person)-[r:KNOWS*1..2]->(n2:Person) RETURN n.name, n2.name").show                   // var expand
//  graph.cypher("MATCH (n:Person)-[:KNOWS]->(n2:Person) RETURN count(n.age), n2").show
  val t = graph.cypher("MATCH (n:Person {name: 'Bob'})-[:KNOWS]->(n2:Person) RETURN n, n2").records.asInstanceOf[CAPFRecords].table.table
  println(session.tableEnv.explain(t))
//  graph.cypher("RETURN coalesce(Null, Null, '3', 'test')").show
//  graph.cypher(
//    """
//      |MATCH (n:Person)
//      |MATCH (p:Person)
//      |RETURN n, p""".stripMargin).show
//  val table = session.sql("SELECT COALESCE(null, null, 3, 5)")
//  println(session.tableEnv.explain(table.table.table))
//  graph.cypher("MATCH (n:Person) WHERE (n)--({age: 29}) RETURN n.name").show                               // exists
//  graph.cypher("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(b {age: 29}) RETURN n.name, b.name").show   // optional match

//  graph.cypher("MATCH (n) RETURN CASE n.age WHEN 26 THEN 'Alice' WHEN 23 THEN 'Bob' ELSE 'other' END AS name").show

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
//  graph.cypher("WITH 'foo' AS bar UNWIND [1, 2, 3] AS x RETURN x, bar").show
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
  session.registerSource(Namespace("csv"), GraphSources.fs(rootPath = csvFolder).csv)

  val purchaseNetwork = session.catalog.graph("csv.products")

  session.cypher(
    """
      |FROM GRAPH csv.products
      |MATCH (c:Customer)
      |RETURN *
    """.stripMargin
  ).show
}

object OrcDemo extends App {

  implicit val session: CAPFSession = CAPFSession.local()

//  val fs = FileSystem.get(new URI("hdfs://foo"))

  val orcFolder = new URI("/home/soeren/Dev/s3/orc").getPath
  session.registerSource(Namespace("orc"), GraphSources.fs(orcFolder).orc)

  PrintRelationalPlan.set

//  session.cypher(
//    """
//      |FROM GRAPH orc.sf1
//      |MATCH (person:Person {id:10995116278874})-[:KNOWS]-(friend:Person)
//      |RETURN person, friend
//    """.stripMargin
//  ).show

  val t = session.cypher(
    """
      |FROM GRAPH orc.sf1
      |MATCH (p:Post)-[:HAS_CREATOR]-(t:Person)
      |RETURN p, t
    """.stripMargin
  ).records.asInstanceOf[CAPFRecords]
  println(session.tableEnv.explain(t.table.table))
  t.show

}

object CircularDemo extends App {

  implicit val session = CAPFSession.local()

  val nodes = Seq(
    (0L, 0L),
    (1L, 1L),
    (2L, 2L),
    (3L, 3L),
    (4L, 4L),
    (5L, 5L),
    (6L, 6L),
    (7L, 7L),
    (8L, 8L),
    (9L, 9L),
    (10L, 10L)
  )

  val rels = Seq(
    (11L, 0L, 1L, 0L),
    (12L, 0L, 3L, 1L),
    (13L, 1L, 6L, 2L),
    (14L, 2L, 6L, 3L),
    (15L, 4L, 1L, 4L),
    (16L, 4L, 3L, 5L),
    (17L, 5L, 4L, 6L),
    (18L, 6L, 2L, 7L),
    (19L, 6L, 5L, 8L),
    (20L, 6L, 7L, 9L),
    (21L, 8L, 5L, 10L),
    (22L, 5L, 9L, 11L),
    (23L, 9L, 10L, 12L),
    (24L, 2L, 1L, 13L)
  )

  val nodeTable = session.tableEnv.fromDataSet(
    session.env.fromCollection(nodes),
    'id, 'prop
  )
  val nodeMapping = NodeMappingBuilder
    .on("id")
    .withImpliedLabel("node")
    .withPropertyKey("prop")
    .build

  val relTable = session.tableEnv.fromDataSet(
    session.env.fromCollection(rels),
    'id, 'source, 'target, 'prop
  )
  val relMapping = RelationshipMappingBuilder
    .on("id")
    .from("source")
    .to("target")
    .relType("relationship")
    .withPropertyKey("prop")
    .build

  val capfNodeTable = CAPFElementTable.create(nodeMapping, nodeTable)
  val capfRelTable = CAPFElementTable.create(relMapping, relTable)

  val graph = session.readFrom(capfNodeTable, capfRelTable)

  PrintRelationalPlan.set()
  val (records, time) =  Measurement.time {
    val records = graph.cypher(
      """
        |MATCH (n0)-[e0]->(n1)-[e1]->(n2)-[e2]->(n3)-[e3]->(n0) RETURN *
      """.stripMargin).records

    records.show
    records
  }
  println(session.tableEnv.explain(records.asCapf.table.table))
  //  records.show
  println(time)

}

object IntegerBug extends App {
  val nodes1 = (0 until 10).foldLeft(Seq.empty[(Long, Int)]) {
    case (acc, i) => acc :+ ((i.toLong, i))
  }
  val nodes2 = (11 until 20).foldLeft(Seq.empty[(Long, String)]) {
    case (acc, i) => acc :+ ((i.toLong, i.toString))
  }

  val edges = Seq(
    (11L, 0L, 1L),
    (12L, 0L, 3L),
    (13L, 1L, 6L),
    (14L, 2L, 6L),
    (15L, 4L, 1L),
    (16L, 4L, 3L),
    (17L, 5L, 4L),
    (18L, 6L, 2L),
    (19L, 6L, 5L),
    (20L, 6L, 7L),
    (21L, 8L, 5L),
    (22L, 5L, 9L),
    (23L, 9L, 10L)
  )

  implicit val session = CAPFSession.local()
  val nodeTable1 = session.tableEnv.fromDataSet(
    session.env.fromCollection(nodes1),
    'node_id, 'prop1
  )
  val nodeTable2 = session.tableEnv.fromDataSet(
    session.env.fromCollection(nodes2),
    'node_id, 'prop2
  )
  val relTable = session.tableEnv.fromDataSet(
    session.env.fromCollection(edges),
    'rel_id, 'start, 'end
  )

  val nodeMapping1 = NodeMappingBuilder.withSourceIdKey("node_id").withImpliedLabel("node1").withPropertyKey("prop1").build
  val nodeMapping2 = NodeMappingBuilder.withSourceIdKey("node_id").withImpliedLabel("node2").withPropertyKey("prop2").build
  val relMapping = RelationshipMappingBuilder.withSourceIdKey("rel_id").withSourceStartNodeKey("start").withSourceEndNodeKey("end").relType("relationship").build

  val capfNodeTable1 = CAPFElementTable.create(nodeMapping1, nodeTable1)
  val capfNodeTable2 = CAPFElementTable.create(nodeMapping2, nodeTable2)
  val capfRelTable = CAPFElementTable.create(relMapping, relTable)

  PrintRelationalPlan.set()
  val graph = session.readFrom(capfNodeTable1, capfRelTable, capfNodeTable2)
  graph.cypher("MATCH (n0)-->(n1) RETURN n0").show
}

object ThesisDemo extends App {

  val musicNodes = Seq(
    (0L, "Metal"),
    (1L, "Jazz")
  )

  val personNodes = Seq(
    (2L, "Alice", 23),
    (3L, "Bob", 26)
  )

  val knowsRels = Seq(
    (4L, 2L, 3L, 2017)
  )

  val likesRels = Seq(
    (5L, 2L, 0L),
    (6L, 2L, 1L),
    (7L, 3L, 1L)
  )

  implicit val session = CAPFSession.local()

  val musicTable = session.tableEnv.fromDataSet(
    session.env.fromCollection(musicNodes),
    'ID, 'genre
  )

  val personTable = session.tableEnv.fromDataSet(
    session.env.fromCollection(personNodes),
    'ID, 'name, 'age
  )

  val knowsTable = session.tableEnv.fromDataSet(
    session.env.fromCollection(knowsRels),
    'ID, 'SOURCE, 'TARGET, 'since
  )

  val likesTable = session.tableEnv.fromDataSet(
    session.env.fromCollection(likesRels),
    'ID, 'SOURCE, 'TARGET
  )

  val musicMapping = NodeMappingBuilder
    .withSourceIdKey("ID")
    .withPropertyKey("genre")
    .withImpliedLabel("Music")
    .build

  val personMapping = NodeMappingBuilder
    .withSourceIdKey("ID")
    .withPropertyKeys("name", "age")
    .withImpliedLabel("Person")
    .build

  val knowsMapping = RelationshipMappingBuilder
    .withSourceIdKey("ID")
    .withSourceStartNodeKey("SOURCE")
    .withSourceEndNodeKey("TARGET")
    .withRelType("KNOWS")
    .withPropertyKey("since")
    .build

  val likesMapping = RelationshipMappingBuilder
    .withSourceIdKey("ID")
    .withSourceStartNodeKey("SOURCE")
    .withSourceEndNodeKey("TARGET")
    .withRelType("LIKES")
    .build

  val music = CAPFElementTable.create(musicMapping, musicTable)
  val person = CAPFElementTable.create(personMapping, personTable)
  val knows = CAPFElementTable.create(knowsMapping, knowsTable)
  val likes = CAPFElementTable.create(likesMapping, likesTable)

  val graph = session.readFrom(music, person, knows, likes)

  PrintIr.set()
  PrintLogicalPlan.set()

  val records = graph.cypher(
    """
      | MATCH (p:Person)-[:LIKES*1..3]->(m:Music)
      | WHERE m.genre = 'Metal'
      | RETURN p.name AS name
    """.stripMargin)

  records.show

}
