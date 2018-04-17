package org.opencypher.flink.test.support.capf

import org.apache.flink.api.scala._
import org.opencypher.flink.CAPFGraph
import org.opencypher.flink.schema.{CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.test.CAPFTestSuite
import org.opencypher.flink.test.support.GraphMatchingTestSupport
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTString
import org.opencypher.okapi.ir.test.support.creation.propertygraph.TestPropertyGraphFactory

class CAPFScanGraphFactoryTest extends CAPFTestSuite with GraphMatchingTestSupport {

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

  val personTable: CAPFNodeTable = CAPFNodeTable(NodeMapping
    .on("ID")
    .withImpliedLabel("Person")
    .withOptionalLabel("Astronaut" -> "IS_ASTRONAUT")
    .withOptionalLabel("Martian" -> "IS_MARTIAN")
    .withPropertyKey("name" -> "NAME"), capf.tableEnv.fromDataSet(
      capf.env.fromCollection(
        Seq(
          (0L, true, false, "Max"),
          (1L, false, true, "Martin")
        )
      )
    )
  )

  val languageTable: CAPFNodeTable = CAPFNodeTable(NodeMapping
      .on("ID")
      .withImpliedLabel("Language")
      .withPropertyKey("title" -> "TITLE"), capf.tableEnv.fromDataSet(
      capf.env.fromCollection(
        Seq(
          (2L, "Swedish"),
          (3L, "German"),
          (4L, "Orbital")
        )
      )
    )
  )

  val knowsScan: CAPFRelationshipTable = CAPFRelationshipTable(RelationshipMapping
    .on("ID")
    .from("SRC").to("DST").relType("KNOWS"), capf.tableEnv.fromDataSet(
      capf.env.fromCollection(
        Seq(
          (0L, 5L, 2L),
          (0L, 6L, 3L),
          (1L, 7L, 3L),
          (1L, 8L, 4L)
        )
      )
    )
  )

  test("testSchema") {
    val propertyGraph = TestPropertyGraphFactory(createQuery)
    CAPFScanGraphFactory(propertyGraph).schema should equal(Schema.empty
      .withNodePropertyKeys("Person", "Astronaut")("name" -> CTString)
      .withNodePropertyKeys("Person", "Martian")("name" -> CTString)
      .withNodePropertyKeys("Language")("title" -> CTString)
      .withRelationshipType("SPEAKS"))
  }

  test("testAsScanGraph") {
    val propertyGraph = TestPropertyGraphFactory(createQuery)
    CAPFScanGraphFactory(propertyGraph) shouldMatch CAPFGraph.create(personTable, languageTable, knowsScan)
  }
}
