package org.opencypher.caps.flink.datasource

import java.net.URI

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.io.{CreateOrFail, Overwrite, PersistMode}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.flink.{CAPFGraph, CAPFSession}
import org.opencypher.caps.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.caps.flink.CAPFConverters._

case class CAPFSessionPropertyGraphDataSource(path: String)(implicit val session: CAPFSession) 
  extends CAPFPropertyGraphDataSource {

  private var currentGraph: Option[CAPFGraph] = None

  override val canonicalURI: URI = URI.create(s"${CAPFSessionPropertyGraphDataSourceFactory.defaultScheme}:$path")

  override def sourceForGraphAt(uri: URI): Boolean =
    uri == canonicalURI

  override def create: CAPFGraph = store(CAPFGraph.empty, CreateOrFail)

  override def graph: CAPFGraph = currentGraph.getOrElse(throw IllegalArgumentException(s"a graph at $canonicalURI"))

  override def schema: Option[Schema] = None

  override def store(graph: PropertyGraph, mode: PersistMode): CAPFGraph = {
    val coscGraph = graph.asCapf
    mode match {
      case Overwrite =>
        currentGraph = Some(coscGraph)
        coscGraph

      case CreateOrFail if currentGraph.isEmpty =>
        currentGraph = Some(coscGraph)
        coscGraph

      case CreateOrFail =>
        throw UnsupportedOperationException(s"Overwriting the session graph")
    }
  }

  override def delete(): Unit =
    currentGraph = None
}
