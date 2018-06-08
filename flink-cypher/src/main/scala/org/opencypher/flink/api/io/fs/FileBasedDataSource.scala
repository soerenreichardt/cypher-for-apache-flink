package org.opencypher.flink.api.io.fs

import org.apache.flink.core.fs.FileSystem
import org.apache.flink.table.api.{Table, TableSchema}
import org.apache.flink.table.expressions.ResolvedFieldReference
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.{CsvTableSource, TableSourceUtil}
import org.opencypher.flink.CAPFSession
import org.opencypher.flink.api.io.AbstractDataSource
import org.opencypher.flink.api.io.json.JsonSerialization
import org.opencypher.flink.api.io.fs.DefaultFileSystem._
import org.opencypher.okapi.api.graph.GraphName

class FileBasedDataSource(
  val rootPath: String,
  val tableStorageFormat: String,
  val customFileSystem: Option[CAPFFileSystem] = None,
  val filesPerTable: Option[Int] = Some(1)
)(implicit session: CAPFSession)
  extends AbstractDataSource with JsonSerialization {

  protected val directoryStructure = DefaultGraphDirectoryStructure(rootPath)

  import directoryStructure._

  protected lazy val fileSystem: CAPFFileSystem = customFileSystem.getOrElse(
    FileSystem.get(FileSystem.getDefaultFsUri))

  protected def listDirectories(path: String): List[String] = fileSystem.listDirectories(path)

  protected def deleteDirectory(path: String): Unit = fileSystem.deleteDirectory(path)

  protected def readFile(path: String): String = fileSystem.readFile(path)

  protected def writeFile(path: String, content: String): Unit = fileSystem.writeFile(path, content)

  protected def readTable(path: String, tableStorageFormat: String, schema: Seq[ResolvedFieldReference]): Table = {
    tableStorageFormat match {
      case "csv" =>
        val csvSource = new CsvTableSource(path, schema.map(_.name).toArray, schema.map(_.resultType).toArray)
        session.tableEnv.registerTableSource(tableStorageFormat, csvSource)
        session.tableEnv.scan(tableStorageFormat)
    }
  }

  protected def writeTable(path: String, tableStorageFormat: String, table: Table): Unit = {
    tableStorageFormat match {
      case "csv" =>
        val csvSink = new CsvTableSink(path)
        table.writeToSink(csvSink)
    }
  }

  override protected def listGraphNames: List[String] = {
    listDirectories(rootPath)
  }

  override protected def deleteGraph(graphName: GraphName): Unit = {
    deleteDirectory(pathToGraphDirectory(graphName))
  }

  override protected def readNodeTable(graphName: GraphName, tableStorageFormat: String, labels: Set[String], tableSchema: Seq[ResolvedFieldReference]): Table = {
    readTable(pathToNodeTable(graphName, labels), tableStorageFormat, tableSchema)
  }

  override protected def writeNodeTable(graphName: GraphName, tableStorageFormat: String, labels: Set[String], table: Table): Unit = {
    writeTable(pathToNodeTable(graphName, labels), tableStorageFormat, table)
  }

  override protected def readRelationshipTable(graphName: GraphName, relKey: String, tableSchema: Seq[ResolvedFieldReference]): Table = {
    readTable(pathToRelationshipTable(graphName, relKey), tableStorageFormat, tableSchema)
  }

  override protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: Table): Unit = {
    writeTable(pathToRelationshipTable(graphName, relKey), tableStorageFormat, table)
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
