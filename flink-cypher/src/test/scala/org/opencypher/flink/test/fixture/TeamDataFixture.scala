package org.opencypher.flink.test.fixture

import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.opencypher.flink.schema.{CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.test.support.RowDebugOutputSupport
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}

import scala.collection.{Bag, mutable}

trait TeamDataFixture extends TestDataFixture with RowDebugOutputSupport {

  self: CAPFSessionFixture =>

  override lazy val dataFixture =
    """
      |       CREATE (a:Person:German {name: "Stefan", luckyNumber: 42})
      |       CREATE (b:Person:Swede  {name: "Mats", luckyNumber: 23})
      |       CREATE (c:Person:German {name: "Martin", luckyNumber: 1337})
      |       CREATE (d:Person:German {name: "Max", luckyNumber: 8})
      |       CREATE (a)-[:KNOWS {since: 2016}]->(b)
      |       CREATE (b)-[:KNOWS {since: 2016}]->(c)
      |       CREATE (c)-[:KNOWS {since: 2016}]->(d)
    """.stripMargin

  override lazy val nbrNodes = 4

  override def nbrRels = 3

  lazy val teamDataGraphNodes: Bag[Row] = Bag(
    Row.of(0L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, 42L: java.lang.Long, "Stefan"),
    Row.of(1L: java.lang.Long, false: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, 23L: java.lang.Long, "Mats"),
    Row.of(2L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, 1337L: java.lang.Long, "Martin"),
    Row.of(3L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, 8L: java.lang.Long, "Max")
  )

  lazy val teamDataGraphRels: Bag[Row] = Bag(
    Row.of(0L: java.lang.Long, 0L: java.lang.Long, "KNOWS", 1L: java.lang.Long, 2016L: java.lang.Long),
    Row.of(1L: java.lang.Long, 1L: java.lang.Long, "KNOWS", 2L: java.lang.Long, 2016L: java.lang.Long),
    Row.of(2L: java.lang.Long, 2L: java.lang.Long, "KNOWS", 3L: java.lang.Long, 2016L: java.lang.Long)
  )

  lazy val csvTestGraphNodes: Bag[Row] = Bag(
    Row.of(1L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, mutable.WrappedArray.make(Array("german", "english")), 42L: java.lang.Long, "Stefan"),
    Row.of(2L: java.lang.Long, true: java.lang.Boolean, false: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, mutable.WrappedArray.make(Array("swedish", "english", "german")), 23L: java.lang.Long, "Mats"),
    Row.of(3L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, mutable.WrappedArray.make(Array("german", "english")), 1337L: java.lang.Long, "Martin"),
    Row.of(4L: java.lang.Long, true: java.lang.Boolean, true: java.lang.Boolean, true: java.lang.Boolean, false: java.lang.Boolean, mutable.WrappedArray.make(Array("german", "swedish", "english")), 8L: java.lang.Long, "Max")
  )

  /**
    * Returns the rels for the test graph in /resources/csv/sn as expected by a
    * [[org.opencypher.flink.CAPFGraph#relationships]] call.
    *
    * @return expected rels
    */
  lazy val csvTestGraphRels: Bag[Row] = Bag(
    Row.of(1L: java.lang.Long, 10L: java.lang.Long, "KNOWS", 2L: java.lang.Long, 2016L: java.lang.Long),
    Row.of(2L: java.lang.Long, 20L: java.lang.Long, "KNOWS", 3L: java.lang.Long, 2017L: java.lang.Long),
    Row.of(3L: java.lang.Long, 30L: java.lang.Long, "KNOWS", 4L: java.lang.Long, 2015L: java.lang.Long)
  )

  /**
    * Returns the rels for the test graph in /resources/csv/sn as expected by a
    * [[org.opencypher.flink.CAPFResult#records]] call.
    *
    * @return expected rels
    */
  lazy val csvTestGraphRelsFromRecords: Bag[Row] = Bag(
    Row.of(10L: java.lang.Long, 1L: java.lang.Long, "KNOWS", 2L: java.lang.Long, 2016L: java.lang.Long),
    Row.of(20L: java.lang.Long, 2L: java.lang.Long, "KNOWS", 3L: java.lang.Long, 2017L: java.lang.Long),
    Row.of(30L: java.lang.Long, 3L: java.lang.Long, "KNOWS", 4L: java.lang.Long, 2015L: java.lang.Long)
  )

  private lazy val personMapping: NodeMapping = NodeMapping
    .on("ID")
    .withImpliedLabel("Person")
    .withOptionalLabel("Swedish" -> "IS_SWEDE")
    .withPropertyKey("name" -> "NAME")
    .withPropertyKey("luckyNumber" -> "NUM")

  protected lazy val personDF: Table = capf.tableEnv.fromDataSet(
    capf.env.fromCollection(
      Seq(
        (1L: java.lang.Long, true: java.lang.Boolean, "Mats", 23L: java.lang.Long),
        (2L: java.lang.Long, false: java.lang.Boolean, "Martin", 42L: java.lang.Long),
        (3L: java.lang.Long, false: java.lang.Boolean, "Max", 1337L: java.lang.Long),
        (4L: java.lang.Long, false: java.lang.Boolean, "Stefan", 9L: java.lang.Long)
      )
    )
  )

  lazy val personTable = CAPFNodeTable(personMapping, personDF)

  private lazy val knowsMapping: RelationshipMapping = RelationshipMapping
    .on("ID").from("SRC").to("DST").relType("KNOWS").withPropertyKey("since" -> "SINCE")

  protected lazy val knowsDF: Table = capf.tableEnv.fromDataSet(
    capf.env.fromCollection(
      Seq(
        (1L: java.lang.Long, 1L: java.lang.Long, 2L: java.lang.Long, 2017L: java.lang.Long),
        (1L: java.lang.Long, 2L: java.lang.Long, 3L: java.lang.Long, 2016L: java.lang.Long),
        (1L: java.lang.Long, 3L: java.lang.Long, 4L: java.lang.Long, 2015L: java.lang.Long),
        (2L: java.lang.Long, 4L: java.lang.Long, 3L: java.lang.Long, 2016L: java.lang.Long),
        (2L: java.lang.Long, 5L: java.lang.Long, 4L: java.lang.Long, 2013L: java.lang.Long),
        (3L: java.lang.Long, 6L: java.lang.Long, 4L: java.lang.Long, 2016L: java.lang.Long)
      )
    )
  )

  lazy val knowsTable = CAPFRelationshipTable(knowsMapping, knowsDF)

  private lazy val programmerMapping = NodeMapping
    .on("ID")
    .withImpliedLabel("Programmer")
    .withImpliedLabel("Person")
    .withPropertyKey("name" -> "NAME")
    .withPropertyKey("luckyNumber" -> "NUM")
    .withPropertyKey("language" -> "LANG")

  private lazy val programmerDF: Table = capf.tableEnv.fromDataSet(
    capf.env.fromCollection(
      Seq(
        (100L: java.lang.Long, "Alice", 42L: java.lang.Long, "C"),
        (200L: java.lang.Long, "Bob", 23L: java.lang.Long, "D"),
        (300L: java.lang.Long, "Eve", 84L: java.lang.Long, "F"),
        (400L: java.lang.Long, "Carl", 49L: java.lang.Long, "R")
      )
    )
  )

  lazy val programmerTable = CAPFNodeTable(programmerMapping, programmerDF)

  private lazy val brogrammerMapping = NodeMapping
    .on("ID")
    .withImpliedLabel("Brogrammer")
    .withImpliedLabel("Person")
    .withPropertyKey("language" -> "LANG")

  private lazy val brogrammerDF = capf.tableEnv.fromDataSet(
    capf.env.fromCollection(
      Seq(
        (100L: java.lang.Long, "Node"),
        (200L: java.lang.Long, "Coffeescript"),
        (300L: java.lang.Long, "Javascript"),
        (400L: java.lang.Long, "Typescript")
      )
    )
  )

  lazy val brogrammerTable = CAPFNodeTable(brogrammerMapping, brogrammerDF)

  private lazy val bookMapping = NodeMapping
    .on("ID")
    .withImpliedLabel("Book")
    .withPropertyKey("title" -> "NAME")
    .withPropertyKey("year" -> "YEAR")

  private lazy val bookDF: Table = capf.tableEnv.fromDataSet(
    capf.env.fromCollection(
      Seq(
        (10L: java.lang.Long, "1984", 1949L: java.lang.Long),
        (20L: java.lang.Long, "Cryptonomicon", 1999L: java.lang.Long),
        (30L: java.lang.Long, "The Eye of the World", 1990L: java.lang.Long),
        (40L: java.lang.Long, "The Circle", 2013L: java.lang.Long)
      )
    )
  )

  lazy val  bookTable = CAPFNodeTable(bookMapping, bookDF)

  private lazy val readsMapping = RelationshipMapping
    .on("ID").from("SRC").to("DST").relType("READS").withPropertyKey("recommends" -> "RECOMMENDS")

  private lazy val readsDF = capf.tableEnv.fromDataSet(
    capf.env.fromCollection(
      Seq(
        (100L: java.lang.Long, 100L: java.lang.Long, 10L: java.lang.Long, true: java.lang.Boolean),
        (200L: java.lang.Long, 200L: java.lang.Long, 40L: java.lang.Long, true: java.lang.Boolean),
        (300L: java.lang.Long, 300L: java.lang.Long, 30L: java.lang.Long, true: java.lang.Boolean),
        (400L: java.lang.Long, 400L: java.lang.Long, 20L: java.lang.Long, false: java.lang.Boolean)
      )
    )
  )

  lazy val readsTable = CAPFRelationshipTable(readsMapping, readsDF)

  private lazy val influencesMapping = RelationshipMapping
    .on("ID").from("SRC").to("DST").relType("INFLUENCES")

  private lazy val influencesDF: Table = capf.tableEnv.fromDataSet(
    capf.env.fromCollection(
      Seq((10L: java.lang.Long, 1000L: java.lang.Long, 20L: java.lang.Long))
    )
  )

  lazy val influencesTable = CAPFRelationshipTable(influencesMapping, influencesDF)

}

