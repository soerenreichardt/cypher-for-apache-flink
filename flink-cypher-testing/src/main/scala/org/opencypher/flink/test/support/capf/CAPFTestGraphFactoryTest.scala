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
package org.opencypher.flink.test.support.capf

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.opencypher.flink.api.io.{CAPFElementTable, CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.test.CAPFTestSuite
import org.opencypher.flink.test.support.GraphMatchingTestSupport
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTString
import org.opencypher.okapi.testing.propertygraph.CreateGraphFactory

abstract class CAPFTestGraphFactoryTest extends CAPFTestSuite with GraphMatchingTestSupport {
  def factory: CAPFTestGraphFactory

  val createQuery: String =
    """
      |CREATE (max:Person:Astronaut {name: "Max"})
      |CREATE (martin:Person:Martian {name: "Martin"})
      |CREATE (swedish:Language {title: "Swedish"})
      |CREATE (german:Language {title: "German"})
      |CREATE (orbital:Language {title: "Orbital"})
      |CREATE (max)-[:SPEAKS]->(swedish)
      |CREATE (max)-[:SPEAKS]->(german)
      |CREATE (martin)-[:SPEAKS]->(german)
      |CREATE (martin)-[:SPEAKS]->(orbital)
    """.stripMargin

  val personAstronautTable: CAPFElementTable = CAPFElementTable.create(NodeMappingBuilder
    .on("ID")
    .withImpliedLabels("Person", "Astronaut")
    .withPropertyKey("name" -> "NAME")
    .withPropertyKey("birthday" -> "BIRTHDAY")
    .build, capf.tableEnv.fromDataSet(
    capf.env.fromCollection(
      Seq(0L, "Max", "1991-07-10")
    ),
    'ID, 'IS_ASTRONAUT, 'IS_MARTIAN, 'NAME)
  )

  val personMartianTable: CAPFElementTable = CAPFElementTable.create(NodeMappingBuilder
      .on("ID")
      .withImpliedLabels("Person", "Martian")
      .withPropertyKey("name" -> "NAME")
      .build, capf.tableEnv.fromDataSet(
    capf.env.fromCollection(
      Seq(1L, "Martin")
    ),
    'ID, 'NAME)
  )

  val languageTable: CAPFElementTable = CAPFElementTable.create(NodeMappingBuilder
    .on("ID")
    .withImpliedLabel("Language")
    .withPropertyKey("title" -> "TITLE")
    .build, capf.tableEnv.fromDataSet(
    capf.env.fromCollection(
      Seq(
        (2L, "Swedish"),
        (3L, "German"),
        (4L, "Orbital")
      )
    ),
    'ID, 'TITLE
  )
  )

  val knowsScan: CAPFElementTable = CAPFElementTable.create(RelationshipMappingBuilder
    .on("ID")
    .from("SRC").to("DST").relType("KNOWS")
    .build, capf.tableEnv.fromDataSet(
    capf.env.fromCollection(
      Seq(
        (0L, 5L, 2L),
        (0L, 6L, 3L),
        (1L, 7L, 3L),
        (1L, 8L, 4L)
      )
    ),
    'SRC, 'ID, 'DST
  )
  )

  test("testSchema") {
    val propertyGraph = CreateGraphFactory(createQuery)
    CAPFScanGraphFactory(propertyGraph).schema should equal(Schema.empty
      .withNodePropertyKeys("Person", "Astronaut")("name" -> CTString)
      .withNodePropertyKeys("Person", "Martian")("name" -> CTString)
      .withNodePropertyKeys("Language")("title" -> CTString)
      .withRelationshipType("SPEAKS"))
  }

  test("testAsScanGraph") {
    val propertyGraph = CreateGraphFactory(createQuery)
    CAPFScanGraphFactory(propertyGraph) shouldMatch capf.graphs.create(personAstronautTable, personMartianTable, languageTable, knowsScan)
  }
}

//class CAPFPatternGraphFactoryTest extends CAPFTestGraphFactoryTest {
//  override def factory: CAPFTestGraphFactory = CAPFPatternGraphFactory
//}

class CAPFScanGraphFactoryTest extends CAPFTestGraphFactoryTest {
  override def factory: CAPFTestGraphFactory = CAPFScanGraphFactory
}