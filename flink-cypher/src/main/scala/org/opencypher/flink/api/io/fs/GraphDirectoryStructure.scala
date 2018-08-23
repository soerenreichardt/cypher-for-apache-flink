package org.opencypher.flink.api.io.fs

import org.apache.flink.core.fs.Path
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.impl.util.StringEncodingUtilities._


trait GraphDirectoryStructure {

  def dataSourceRootPath: String

  def pathToGraphDirectory(graphName: GraphName): String

  def pathToGraphSchema(graphName: GraphName): String

  def pathToCAPSMetaData(graphName: GraphName): String

  def pathToNodeTable(graphName: GraphName, labels: Set[String]): String

  def pathToRelationshipTable(graphName: GraphName, relKey: String): String

}

object DefaultGraphDirectoryStructure {

  implicit class StringPath(val path: String) extends AnyVal {
    def /(segment: String): String = s"$path$pathSeparator$segment"
  }

  implicit class GraphPath(graphName: GraphName) {
    def path: String = graphName.value.replace(".", pathSeparator)
  }

  val pathSeparator: String = Path.SEPARATOR

  val schemaFileName: String = "propertyGraphSchema.json"

  val capsMetaDataFileName: String = "capsGraphMetaData.json"

  val nodeTablesDirectory = "nodes"

  val relationshipTablesDirectory = "relationships"

  def nodeTableDirectory(labels: Set[String]): String = labels.toSeq.sorted.mkString("_").encodeSpecialCharacters

  def relKeyTableDirectory(relKey: String): String = relKey.encodeSpecialCharacters

}

case class DefaultGraphDirectoryStructure(dataSourceRootPath: String) extends GraphDirectoryStructure {

  import DefaultGraphDirectoryStructure._

  override def pathToGraphDirectory(graphName: GraphName): String = {
    dataSourceRootPath / graphName.path
  }

  override def pathToGraphSchema(graphName: GraphName): String = {
    pathToGraphDirectory(graphName) / schemaFileName
  }

  override def pathToCAPSMetaData(graphName: GraphName): String = {
    pathToGraphDirectory(graphName) / capsMetaDataFileName
  }

  override def pathToNodeTable(graphName: GraphName, labels: Set[String]): String = {
    pathToGraphDirectory(graphName) / nodeTablesDirectory / nodeTableDirectory(labels)
  }

  override def pathToRelationshipTable(graphName: GraphName, relKey: String): String = {
    pathToGraphDirectory(graphName) / relationshipTablesDirectory / relKeyTableDirectory(relKey)
  }

}