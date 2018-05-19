package org.opencypher.caps.flink.datasource

import java.net.URI

import org.opencypher.caps.api.graph.CypherSession
import org.opencypher.caps.flink.CAPFSession
import org.opencypher.caps.impl.exception.IllegalArgumentException
import org.opencypher.caps.flink.CAPFConverters._

abstract class CAPFPropertyGraphDataSourceFactoryImpl(val companion: CAPFGraphSourceFactoryCompanion)
  extends CAPFPropertyGraphDataSourceFactory {
  
  override final val name: String = getClass.getSimpleName

  override final def schemes: Set[String] = companion.supportedSchemes

  override final def sourceFor(uri: URI)(implicit cypherSession: CypherSession): CAPFPropertyGraphDataSource = {
    implicit val capfSession = cypherSession.asCapf
    if (schemes.contains(uri.getScheme)) sourceForURIWithSupportedScheme(uri)
    else throw IllegalArgumentException(s"a supported scheme: ${schemes.toSeq.sorted.mkString(", ")}", uri.getScheme)
  }

  protected def sourceForURIWithSupportedScheme(uri: URI)(implicit coscSession: CAPFSession): CAPFPropertyGraphDataSource

}
