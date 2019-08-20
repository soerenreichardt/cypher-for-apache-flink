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
package org.opencypher.flink.api.io

import java.util.concurrent.Executors

import org.apache.flink.table.api.Table
import org.apache.flink.table.expressions.ResolvedFieldReference
import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.api.io.metadata.CAPFGraphMetaData
import org.opencypher.flink.api.io.util.CAPFGraphExport._
import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.flink.impl.io.CAPFPropertyGraphDataSource
import org.opencypher.flink.schema.CAPFSchema
import org.opencypher.flink.schema.CAPFSchema._
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.impl.exception.GraphNotFoundException

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success}

abstract class AbstractPropertyGraphDataSource extends CAPFPropertyGraphDataSource {

  implicit val capf: CAPFSession

  def tableStorageFormat: StorageFormat

  protected var schemaCache: Map[GraphName, CAPFSchema] = Map.empty

  protected var graphNameCache: Set[GraphName] = listGraphNames.map(GraphName).toSet

  protected def listGraphNames: List[String]

  protected def deleteGraph(graphName: GraphName): Unit

  protected def readSchema(graphName: GraphName): CAPFSchema

  protected def writeSchema(graphName: GraphName, schema: CAPFSchema): Unit

  protected def readCAPFGraphMetaData(graphName: GraphName): CAPFGraphMetaData

  protected def writeCAPFGraphMetaData(graphName: GraphName, capfGraphMetaData: CAPFGraphMetaData): Unit

  protected def readNodeTable(graphName: GraphName, labels: Set[String], tableSchema: Seq[ResolvedFieldReference]): Table

  protected def writeNodeTable(graphName: GraphName, labels: Set[String], table: Table): Unit

  protected def readRelationshipTable(graphName: GraphName, relKey: String, tableSchema: Seq[ResolvedFieldReference]): Table

  protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: Table): Unit

  override def graphNames: Set[GraphName] = graphNameCache

  override def hasGraph(graphName: GraphName): Boolean = graphNameCache.contains(graphName)

  override def delete(graphName: GraphName): Unit = {
    schemaCache -= graphName
    graphNameCache -= graphName
    deleteGraph(graphName)
  }

  override def graph(graphName: GraphName): PropertyGraph = {
    if (!hasGraph(graphName)) {
      throw GraphNotFoundException(s"Graph with name '$graphName'")
    } else {
      val capfSchema: CAPFSchema = schema(graphName).get
      val capfMetaData: CAPFGraphMetaData = readCAPFGraphMetaData(graphName)
      val nodeTables = capfSchema.allCombinations.map { combo =>
        val table = readNodeTable(graphName, combo, capfSchema.canonicalNodeFieldReference(combo))
        CAPFNodeTable(combo, table)
      }

      val relTables = capfSchema.relationshipTypes.map { relType =>
                val table = readRelationshipTable(graphName, relType, capfSchema.canonicalRelFieldReference(relType))
        CAPFRelationshipTable(relType, table)
      }
      if (nodeTables.isEmpty) {
        capf.graphs.empty
      } else {
        capf.graphs.create(Some(capfSchema), (nodeTables ++ relTables).toSeq: _*)
      }
    }
  }

  override def schema(graphName: GraphName): Option[CAPFSchema] = {
    if (schemaCache.contains(graphName)) {
      schemaCache.get(graphName)
    } else {
      val s = readSchema(graphName)
      schemaCache += graphName -> s
      Some(s)
    }
  }

  override def store(graphName: GraphName, graph: PropertyGraph): Unit = {
    checkStorable(graphName)

    // TODO: check if this is the expected value | probably the parallelism of a worker is needed
    val poolSize = capf.env.getParallelism

    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(poolSize))

    try {
      val relationalGraph = graph.asCapf

      val schema = relationalGraph.schema.asCapf
      schemaCache += graphName -> schema
      graphNameCache += graphName
      writeCAPFGraphMetaData(graphName, CAPFGraphMetaData(tableStorageFormat.name))
      writeSchema(graphName, schema)

      val nodeWrites = schema.labelCombinations.combos.map { combo =>
        Future {
          writeNodeTable(graphName, combo, relationalGraph.canonicalNodeTable(combo))
        }
      }

      val relWrites = schema.relationshipTypes.map { relType =>
        Future {
          writeRelationshipTable(graphName, relType, relationalGraph.canonicalRelationshipTable(relType))
        }
      }

      waitForWriteCompletion(nodeWrites)
      waitForWriteCompletion(relWrites)
    } finally {
      executionContext.shutdown()
    }
  }

  protected def waitForWriteCompletion(writeFutures: Set[Future[Unit]])(implicit ec: ExecutionContext): Unit = {
    writeFutures.foreach { writeFuture =>
      Await.ready(writeFuture, Duration.Inf)
      writeFuture.onComplete {
        case Success(_) =>
        case Failure(e) => throw e
      }
    }
  }

}