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
// tag::full-example[]
package org.opencypher.morpheus.examples

import org.apache.spark.sql.SparkSession
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{Node, Relationship, RelationshipType}
import org.opencypher.morpheus.util.App

/**
  * Demonstrates basic usage of the Morpheus API by loading an example network via Scala case classes and running a Cypher
  * query on it.
  */
object CaseClassExample extends App {

  // 1) Create Morpheus session
  implicit val morpheus: MorpheusSession = MorpheusSession.create(SparkSession.builder().master("local[*]").getOrCreate())

  // 2) Load social network data via case class instances
  val socialNetwork = morpheus.readFrom(SocialNetworkData.persons, SocialNetworkData.friendships)

  // 3) Query graph with Cypher
  val results = socialNetwork.cypher(
    """|MATCH (a:Person)-[r:FRIEND_OF]->(b)
       |RETURN a.name, b.name, r.since
       |ORDER BY a.name""".stripMargin
  )

  // 4) Print result to console
  results.show
}

/**
  * Specify schema and data with case classes.
  */
object SocialNetworkData {

  case class Person(id: Long, name: String, age: Int) extends Node

  @RelationshipType("FRIEND_OF")
  case class Friend(id: Long, source: Long, target: Long, since: String) extends Relationship

  val alice = Person(0, "Alice", 10)
  val bob = Person(1, "Bob", 20)
  val carol = Person(2, "Carol", 15)

  val persons = List(alice, bob, carol)
  val friendships = List(Friend(0, alice.id, bob.id, "23/01/1987"), Friend(1, bob.id, carol.id, "12/12/2009"))
}
// end::full-example[]
