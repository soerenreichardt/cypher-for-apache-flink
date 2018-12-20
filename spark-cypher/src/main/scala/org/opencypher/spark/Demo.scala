package org.opencypher.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}
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
