/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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

import java.sql._
import java.time.LocalDate

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.opencypher.flink.api.io.{CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.api.{CAPFSession, GraphSources}
import org.opencypher.okapi.api.configuration.Configuration.PrintTimings
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.ir.api.configuration.IrConfiguration.PrintIr
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintRelationalPlan

object Demo extends App {

  val session = CAPFSession.local()

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

  val graph = session.readFrom(nodeTable, relTable)

  PrintIr.set()
  PrintLogicalPlan.set()
  PrintRelationalPlan.set()
  PrintTimings.set()
//  graph.cypher("MATCH (n:person) RETURN n.name").show
//  val planning = Measurement.time(graph.cypher("MATCH (n:Person)-[r:KNOWS]->(n2:Person) WHERE n.age >= 26 RETURN n.age AS age"))
//  println("Planning: " + planning._2)
//  val translation = Measurement.time(planning._1.getRecords.asCapf.table.toDataSet[Row])
//  println("Translation: " + translation._2)
//  val execution = Measurement.time(translation._1.collect())
//  println("Execution: " +  execution._2)
//  println("Flink execution: " + session.env.getLastJobExecutionResult.getNetRuntime)
//  println(session.tableEnv.explain(planning._1.getRecords.asCapf.table))
  graph.cypher("MATCH (n:Person)-[r:KNOWS*1..2]->(n2:Person) RETURN n.name, n2.name").show                   // var expand
//  graph.cypher("MATCH (n:Person) WHERE (n)--({age: 29}) RETURN n.name").show                               // exists
//  graph.cypher("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(b {age: 29}) RETURN n.name, b.name").show   // optional match

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

  val orcFolder = getClass.getResource("/orc").getPath
  session.registerSource(Namespace("orc"), GraphSources.fs(orcFolder).orc)

  session.cypher(
    """
      |FROM GRAPH orc.sf1
      |MATCH (n:Person)
      |RETURN n.firstName
    """.stripMargin
  ).show
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

  val session = CAPFSession.local()

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

  val musicMapping = NodeMapping
    .withSourceIdKey("ID")
    .withPropertyKey("genre")
    .withImpliedLabel("Music")

  val personMapping = NodeMapping
    .withSourceIdKey("ID")
    .withPropertyKeys("name", "age")
    .withImpliedLabel("Person")

  val knowsMapping = RelationshipMapping
    .withSourceIdKey("ID")
    .withSourceStartNodeKey("SOURCE")
    .withSourceEndNodeKey("TARGET")
    .withRelType("KNOWS")
    .withPropertyKey("since")

  val likesMapping = RelationshipMapping
    .withSourceIdKey("ID")
    .withSourceStartNodeKey("SOURCE")
    .withSourceEndNodeKey("TARGET")
    .withRelType("LIKES")

  val music = CAPFNodeTable.fromMapping(musicMapping, musicTable)
  val person = CAPFNodeTable.fromMapping(personMapping, personTable)
  val knows = CAPFRelationshipTable.fromMapping(knowsMapping, knowsTable)
  val likes = CAPFRelationshipTable.fromMapping(likesMapping, likesTable)

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
