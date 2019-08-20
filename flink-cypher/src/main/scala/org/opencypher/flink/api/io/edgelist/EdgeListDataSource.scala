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
package org.opencypher.flink.api.io.edgelist

import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.apache.flink.table.sources.CsvTableSource
import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.flink.api.io.{CAPFNodeTable, CAPFRelationshipTable, GraphElement}
import org.opencypher.flink.impl.TableOps._
import org.opencypher.flink.schema.CAPFSchema
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.{PropertyGraphSchema, PropertyKeys}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException

object EdgeListDataSource {

  val NODE_LABEL = "V"

  val REL_TYPE = "E"

  val GRAPH_NAME = GraphName("graph")

  val SCHEMA: PropertyGraphSchema = CAPFSchema.empty
    .withNodePropertyKeys(Set(NODE_LABEL), PropertyKeys.empty)
    .withRelationshipPropertyKeys(REL_TYPE, PropertyKeys.empty)
}

case class EdgeListDataSource(path: String, options: Map[String, String] = Map.empty)(implicit capf: CAPFSession)
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

    capf.tableEnv.registerTableSource("relScanCsv", relTableSource)
    val rawRels = capf.tableEnv.scan("relScanCsv")
      .safeAddIdColumn(GraphElement.sourceIdKey)

    val rawNodes = rawRels
      .select(UnresolvedFieldReference(sourceStartNodeKey) as Symbol(GraphElement.sourceIdKey))
      .union(rawRels.select(UnresolvedFieldReference(sourceEndNodeKey) as Symbol(GraphElement.sourceIdKey)))
      .distinct()

    capf.graphs.create(CAPFNodeTable(Set(EdgeListDataSource.NODE_LABEL), rawNodes), CAPFRelationshipTable(EdgeListDataSource.REL_TYPE, rawRels))

  }

  override def schema(name: GraphName): Option[PropertyGraphSchema] = Some(EdgeListDataSource.SCHEMA)

  override def store(name: GraphName, graph: PropertyGraph): Unit =
    throw UnsupportedOperationException("Storing an edge list is not supported")

  override def delete(name: GraphName): Unit =
    throw UnsupportedOperationException("Deleting an edge list is not supported")

  override def graphNames: Set[GraphName] = Set(EdgeListDataSource.GRAPH_NAME)
}
