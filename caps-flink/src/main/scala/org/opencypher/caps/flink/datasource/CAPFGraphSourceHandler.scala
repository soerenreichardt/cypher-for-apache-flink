package org.opencypher.caps.flink.datasource

import java.net.URI

import org.opencypher.caps.api.io.PropertyGraphDataSource
import org.opencypher.caps.flink.CAPFSession
import org.opencypher.caps.impl.exception.IllegalArgumentException

case class CAPFGraphSourceHandler(
                                 sessionGraphSourceFactory: CAPFSessionPropertyGraphDataSourceFactory,
                                 additionalGraphSourceFactories: Set[CAPFPropertyGraphDataSourceFactory]) {

  private val factoriesByScheme: Map[String, CAPFPropertyGraphDataSourceFactory] = {
    val allFactories = Seq(sessionGraphSourceFactory)
    val entries = allFactories.flatMap(factory => factory.schemes.map(scheme => scheme -> factory))
    if (entries.size == entries.map(_._1).size)
      entries.toMap
    else
      throw IllegalArgumentException(
        "at most one graph source factory per URI scheme",
        s"factories for schemes: ${allFactories.map(factory => factory.name -> factory.schemes.mkString("[", ", ", "]")).mkString(",")}"
      )
  }

  def mountSourceAt(source: CAPFPropertyGraphDataSource, uri: URI)(implicit coscSession: CAPFSession): Unit =
    sessionGraphSourceFactory.mountSourceAt(source, uri)

  def unmountAll(implicit coscSession: CAPFSession): Unit =
    sessionGraphSourceFactory.unmountAll(coscSession)

  def sourceAt(uri: URI)(implicit coscSession: CAPFSession): PropertyGraphDataSource =
    optSourceAt(uri).getOrElse(throw IllegalArgumentException(s"graph source for URI: $uri"))

  def optSourceAt(uri: URI)(implicit coscSession: CAPFSession): Option[PropertyGraphDataSource] =
    factoriesByScheme
      .get(uri.getScheme)
      .map(_.sourceFor(uri))
}