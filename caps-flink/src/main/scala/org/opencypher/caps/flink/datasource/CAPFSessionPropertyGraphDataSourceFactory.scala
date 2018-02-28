package org.opencypher.caps.flink.datasource

import java.net.URI
import java.util.concurrent.ConcurrentHashMap

import org.opencypher.caps.api.graph.CypherSession
import org.opencypher.caps.flink.CAPFSession
import org.opencypher.caps.impl.exception.{IllegalArgumentException, UnsupportedOperationException}

import scala.collection.JavaConversions._

case object CAPFSessionPropertyGraphDataSourceFactory extends CAPFGraphSourceFactoryCompanion(CypherSession.sessionGraphSchema)

case class CAPFSessionPropertyGraphDataSourceFactory()
  extends CAPFPropertyGraphDataSourceFactoryImpl(CAPFSessionPropertyGraphDataSourceFactory) {

  val mountPoints: collection.concurrent.Map[String, CAPFPropertyGraphDataSource] = {
    new ConcurrentHashMap[String, CAPFPropertyGraphDataSource]()
  }

  def mountSourceAt(existingSource: CAPFPropertyGraphDataSource, uri: URI)(implicit capsSession: CAPFSession): Unit =
    if (schemes.contains(uri.getScheme))
      withValidPath(uri) { (path: String) =>
        mountPoints.get(path) match {
          case Some(source) =>
            throw UnsupportedOperationException(s"Overwriting session graph at $source")

          case _ =>
            mountPoints.put(path, existingSource)
        }
      } else throw IllegalArgumentException(s"supported scheme: ${schemes.mkString("[", ", ", "]")}", uri.getScheme)

  def unmountAll(implicit capsSession: CypherSession): Unit =
    mountPoints.clear()

  override protected def sourceForURIWithSupportedScheme(uri: URI)(implicit capsSession: CAPFSession): CAPFPropertyGraphDataSource =
    withValidPath(uri) { (path: String) =>
      mountPoints.get(path) match {
        case Some(source) =>
          source

        case _ =>
          val newSource = CAPFSessionPropertyGraphDataSource(path)
          mountPoints.put(path, newSource)
          newSource
      }
    }

  private def withValidPath[T](uri: URI)(f: String => T): T = {
    val path = uri.getPath
    if (uri.getUserInfo != null ||
      uri.getHost != null ||
      uri.getPort != -1 ||
      uri.getQuery != null ||
      uri.getAuthority != null ||
      uri.getFragment != null ||
      path == null ||
      !path.startsWith("/"))
      throw IllegalArgumentException(s"a valid URI for use by $name", uri)
    else
      f(path)
  }
}