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
package org.opencypher.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.impl.util.Measurement
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintRelationalPlan
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}

import scala.collection.JavaConverters._

object CircularDemo extends App {

  implicit val session = CAPSSession.local()

  val nodes = List(
    Row(0L, 0L),
    Row(1L, 1L),
    Row(2L, 2L),
    Row(3L, 3L),
    Row(4L, 4L),
    Row(5L, 5L),
    Row(6L, 6L),
    Row(7L, 7L),
    Row(8L, 8L),
    Row(9L, 9L),
    Row(10L, 10L)
  ).asJava

  val rels = List(
    Row(11L, 0L, 1L, 0L),
    Row(12L, 0L, 3L, 1L),
    Row(13L, 1L, 6L, 2L),
    Row(14L, 2L, 6L, 3L),
    Row(15L, 4L, 1L, 4L),
    Row(16L, 4L, 3L, 5L),
    Row(17L, 5L, 4L, 6L),
    Row(18L, 6L, 2L, 7L),
    Row(19L, 6L, 5L, 8L),
    Row(20L, 6L, 7L, 9L),
    Row(21L, 8L, 5L, 10L),
    Row(22L, 5L, 9L, 11L),
    Row(23L, 9L, 10L, 12L)
  ).asJava

  val nodeDF = session.sparkSession.createDataFrame(nodes, StructType(List(
    StructField("id", LongType, false),
    StructField("prop", LongType, false)
  )))
  val nodeMapping = NodeMapping
    .on("id")
    .withImpliedLabel("node")
    .withPropertyKey("prop")

  val relDf = session.sparkSession.createDataFrame(rels, StructType(List(
    StructField("id", LongType, false),
    StructField("source", LongType, false),
    StructField("target", LongType, false),
    StructField("prop", LongType, false)
  )))
  val relMapping = RelationshipMapping
    .on("id")
    .from("source")
    .to("target")
    .relType("relationship")
    .withPropertyKey("prop")


  val graph = session.readFrom(CAPSNodeTable(nodeMapping, nodeDF), CAPSRelationshipTable(relMapping, relDf))

  PrintRelationalPlan.set()
  val (_, time) =  Measurement.time(graph.cypher(
    """
      |MATCH (n0)-[e0]->(n1)-[e1]->(n2)-[e2]->(n3)-[e3]->(n0) RETURN *
    """.stripMargin).records.show)
  println(time)

}

object IntegerBug1 extends App {
  implicit val session = CAPSSession.local()
  val nodes1 = (0 until 10).foldLeft(List.empty[Row]) {
    case (acc, i) => acc :+ Row(i.toLong, i)
  }.asJava
  val nodes2 = (11 until 20).foldLeft(List.empty[Row]) {
    case (acc, i) => acc :+ Row(i.toLong, i.toString)
  }.asJava

  val edges = List(
    Row(11L, 0L, 1L),
    Row(12L, 0L, 3L),
    Row(13L, 1L, 6L),
    Row(14L, 2L, 6L),
    Row(15L, 4L, 1L),
    Row(16L, 4L, 3L),
    Row(17L, 5L, 4L),
    Row(18L, 6L, 2L),
    Row(19L, 6L, 5L),
    Row(20L, 6L, 7L),
    Row(21L, 8L, 5L),
    Row(22L, 5L, 9L),
    Row(23L, 9L, 10L)
  ).asJava

  val nodeDf1 = session.sparkSession.createDataFrame(nodes1, StructType(List(
    StructField("node_id", LongType, false),
    StructField("prop1", IntegerType, false)
  )))

  val nodeDf2 = session.sparkSession.createDataFrame(nodes2, StructType(List(
    StructField("node_id", LongType, false),
    StructField("prop2", StringType, false)
  )))

  val edgeDf = session.sparkSession.createDataFrame(edges, StructType(List(
    StructField("rel_id", LongType, false),
    StructField("start", LongType, false),
    StructField("end", LongType, false)
  )))

  val nodeMapping1 = NodeMapping.withSourceIdKey("node_id").withImpliedLabel("node1").withPropertyKey("prop1")
  val nodeMapping2 = NodeMapping.withSourceIdKey("node_id").withImpliedLabel("node2").withPropertyKey("prop2")
  val relMapping = RelationshipMapping.withSourceIdKey("rel_id").withSourceStartNodeKey("start").withSourceEndNodeKey("end").relType("relationship")

  val capsNodeTable1 = CAPSNodeTable.fromMapping(nodeMapping1, nodeDf1)
  val capsNodeTable2 = CAPSNodeTable.fromMapping(nodeMapping2, nodeDf2)
  val capsRelTable = CAPSRelationshipTable.fromMapping(relMapping, edgeDf)

  val graph = session.readFrom(capsNodeTable1, capsRelTable, capsNodeTable2)
  graph.cypher("MATCH (n0)--(n1) RETURN n0").show

}
