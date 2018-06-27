package org.opencypher.flink.api.io.edgelist

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.apache.flink.table.sources.CsvTableSource
import org.opencypher.flink.api.io.{CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.api.io.GraphEntity.sourceIdKey
import org.opencypher.flink.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.flink.impl.{CAPFGraph, CAPFSession}
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink.schema.CAPFSchema
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.{PropertyKeys, Schema}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException

object EdgeListDataSource {

  val NODE_LABEL = "V"

  val REL_TYPE = "E"

  val GRAPH_NAME = GraphName("graph")

  val SCHEMA: Schema = CAPFSchema.empty
    .withNodePropertyKeys(Set(NODE_LABEL), PropertyKeys.empty)
    .withRelationshipPropertyKeys(REL_TYPE, PropertyKeys.empty)
}

case class EdgeListDataSource(path: String, options: Map[String, String] = Map.empty)(implicit session: CAPFSession)
  extends PropertyGraphDataSource {

  override def hasGraph(name: GraphName): Boolean = name == EdgeListDataSource.GRAPH_NAME

  override def graph(name: GraphName): PropertyGraph = {
    val relTableSource = CsvTableSource.builder()
      .path(path)
      .field(sourceStartNodeKey, Types.LONG)
      .field(sourceEndNodeKey,  Types.LONG)
      .fieldDelimiter(options.get("sep").get)
      .commentPrefix(options.get("comment").get)
      .build()
    session.tableEnv.registerTableSource("relScanCsv", relTableSource)
    val rawRels = session.tableEnv.scan("relScanCsv").safeAddIdColumn()

    val rawNodes = rawRels
      .select(UnresolvedFieldReference(sourceStartNodeKey) as Symbol(sourceIdKey))
      .union(rawRels.select(UnresolvedFieldReference(sourceEndNodeKey) as Symbol(sourceIdKey)))
      .distinct()

    CAPFGraph.create(CAPFNodeTable(Set(EdgeListDataSource.NODE_LABEL), rawNodes), CAPFRelationshipTable(EdgeListDataSource.REL_TYPE, rawRels))

  }

  override def schema(name: GraphName): Option[Schema] = Some(EdgeListDataSource.SCHEMA)

  override def store(name: GraphName, graph: PropertyGraph): Unit =
    throw UnsupportedOperationException("Storing an edge list is not supported")

  override def delete(name: GraphName): Unit =
    throw UnsupportedOperationException("Deleting an edge list is not supported")

  override def graphNames: Set[GraphName] = Set(EdgeListDataSource.GRAPH_NAME)
}
