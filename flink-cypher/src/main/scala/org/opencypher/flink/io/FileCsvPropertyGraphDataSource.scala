package org.opencypher.flink.io

import java.io.File
import java.nio.file.{Files, Paths}

import org.opencypher.flink.CAPFSession
import org.opencypher.flink.datasource.CAPFPropertyGraphDataSource
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema

import scala.collection.JavaConverters._

class FileCsvPropertyGraphDataSource(rootPath: String)(implicit val session: CAPFSession)
  extends CAPFPropertyGraphDataSource {

  override def graph(name: GraphName): PropertyGraph = CsvGraphLoader(graphPath(name)).load

  override def schema(name: GraphName): Option[Schema] = None

  override def store(name: GraphName, graph: PropertyGraph): Unit = ???

  override def delete(name: GraphName): Unit =
    if (hasGraph(name)) Files.delete(Paths.get(graphPath(name)))

  override def graphNames: Set[GraphName] = Files.list(Paths.get(rootPath)).iterator().asScala
    .filter(p => Files.isDirectory(p))
    .map(p => p.getFileName.toString)
    .map(GraphName)
    .toSet

  override def hasGraph(name: GraphName): Boolean = Files.exists(Paths.get(graphPath(name)))

  private def graphPath(name: GraphName): String = s"$rootPath${File.separator}$name"

}
