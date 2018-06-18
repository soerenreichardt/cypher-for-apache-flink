package org.opencypher.flink.test.support.capf

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.opencypher.flink.api.io.{CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.impl.CAPFGraph
import org.opencypher.flink.test.CAPFTestSuite
import org.opencypher.flink.test.support.GraphMatchingTestSupport
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
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

  val personTable: CAPFNodeTable = CAPFNodeTable.fromMapping(NodeMapping
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
      ),
      'ID, 'IS_ASTRONAUT, 'IS_MARTIAN, 'NAME
    )
  )

  val languageTable: CAPFNodeTable = CAPFNodeTable.fromMapping(NodeMapping
      .on("ID")
      .withImpliedLabel("Language")
      .withPropertyKey("title" -> "TITLE"), capf.tableEnv.fromDataSet(
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

  val knowsScan: CAPFRelationshipTable = CAPFRelationshipTable.fromMapping(RelationshipMapping
    .on("ID")
    .from("SRC").to("DST").relType("KNOWS"), capf.tableEnv.fromDataSet(
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
    CAPFScanGraphFactory(propertyGraph) shouldMatch CAPFGraph.create(personTable, languageTable, knowsScan)
  }
}

//class CAPFPatternGraphFactoryTest extends CAPFTestGraphFactoryTest {
//  override def factory: CAPFTestGraphFactory = CAPFPatternGraphFactory
//}

class CAPFScanGraphFactoryTest extends CAPFTestGraphFactoryTest {
  override def factory: CAPFTestGraphFactory = CAPFScanGraphFactory
}