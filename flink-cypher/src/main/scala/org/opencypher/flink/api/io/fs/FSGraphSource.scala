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
package org.opencypher.flink.api.io.fs

import java.net.URI
import java.util.UUID

//import org.apache.flink.core.fs.FileSystem
import org.apache.hadoop.fs.FileSystem
import org.apache.flink.orc.OrcTableSource
import org.apache.flink.table.api.Table
import org.apache.flink.table.expressions.ResolvedFieldReference
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource
import org.apache.hadoop.conf.Configuration
import org.apache.orc.TypeDescription
import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.api.io.json.JsonSerialization
import org.opencypher.flink.api.io.{AbstractPropertyGraphDataSource, StorageFormat}
import org.opencypher.flink.impl.convert.FlinkConversions._
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.flink.api.io.fs.FlinkFSHelpers._

class FSGraphSource(
  val rootPath: String,
  val tableStorageFormat: StorageFormat,
  val filesPerTable: Option[Int] = Some(1)
)(override implicit val capf: CAPFSession)
  extends AbstractPropertyGraphDataSource with JsonSerialization {

  protected val directoryStructure = DefaultGraphDirectoryStructure(rootPath)

  import directoryStructure._

  private val configuration = new Configuration

  protected lazy val fileSystem: FileSystem = {
    FileSystem.get(new URI(rootPath), configuration)
  }

  protected def listDirectories(path: String): List[String] = fileSystem.listDirectories(path)

  protected def deleteDirectory(path: String): Unit = fileSystem.deleteDirectory(path)

  protected def readFile(path: String): String = fileSystem.readFile(path)

  protected def writeFile(path: String, content: String): Unit = fileSystem.writeFile(path, content)

  protected def readTable(path: String, schema: Seq[ResolvedFieldReference]): Table = {
    val tableSourceName = tableStorageFormat + "#" + UUID.randomUUID()
    tableStorageFormat.name match {
      case "csv" =>
        readFromCsv(path, schema, tableSourceName)
      case "orc" =>
        readFromOrc(path, schema, tableSourceName)
    }
  }

  private def readFromCsv(path: String, schema: Seq[ResolvedFieldReference], tableSourceName: String): Table = {
    val csvSource = new CsvTableSource(path, schema.map(_.name).toArray, schema.map(_.resultType).toArray)
    capf.tableEnv.registerTableSource(tableSourceName, csvSource)
    capf.tableEnv.scan(tableSourceName)
  }

  private def readFromOrc(path: String, schema: Seq[ResolvedFieldReference], tableSourceName: String): Table = {
    val typeDescription = schema.foldLeft(new TypeDescription(TypeDescription.Category.STRUCT)) {
      case (acc, fieldRef) => acc.addField(fieldRef.name, fieldRef.resultType.getOrcType)
    }
    val orcSource = OrcTableSource.builder()
      .path(path)
      .forOrcSchema(typeDescription)
      .withConfiguration(configuration)
      .build()
    capf.tableEnv.registerTableSource(tableSourceName, orcSource)
    capf.tableEnv.scan(tableSourceName)
  }

  protected def writeTable(path: String, table: Table): Unit = {
    tableStorageFormat.name match {
      case "csv" =>
        val csvSink = new CsvTableSink(path)
        capf.tableEnv.registerTableSink("CsvTableSink", csvSink)
        table.insertInto("CsvTableSink")
    }
  }

  override protected def listGraphNames: List[String] = {
    listDirectories(rootPath)
  }

  override protected def deleteGraph(graphName: GraphName): Unit = {
    deleteDirectory(pathToGraphDirectory(graphName))
  }

  override protected def readNodeTable(graphName: GraphName, labels: Set[String], tableSchema: Seq[ResolvedFieldReference]): Table = {
    readTable(pathToNodeTable(graphName, labels), tableSchema)
  }

  override protected def writeNodeTable(graphName: GraphName, labels: Set[String], table: Table): Unit = {
    writeTable(pathToNodeTable(graphName, labels), table)
  }

  override protected def readRelationshipTable(graphName: GraphName, relKey: String, tableSchema: Seq[ResolvedFieldReference]): Table = {
    readTable(pathToRelationshipTable(graphName, relKey), tableSchema)
  }

  override protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: Table): Unit = {
    writeTable(pathToRelationshipTable(graphName, relKey), table)
  }

  override protected def readJsonSchema(graphName: GraphName): String = {
    readFile(pathToGraphSchema(graphName))
  }

  override protected def writeJsonSchema(graphName: GraphName, schema: String): Unit = {
    writeFile(pathToGraphSchema(graphName), schema)
  }

  override protected def readJsonCAPFGraphMetaData(graphName: GraphName): String = {
    readFile(pathToCAPSMetaData(graphName))
  }

  override protected def writeJsonCAPFGraphMetaData(graphName: GraphName, capfGraphMetaData: String): Unit = {
    writeFile(pathToCAPSMetaData(graphName), capfGraphMetaData)
  }
}
