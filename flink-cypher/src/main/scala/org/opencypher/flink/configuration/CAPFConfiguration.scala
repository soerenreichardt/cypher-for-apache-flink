package org.opencypher.flink.configuration

import org.opencypher.okapi.impl.configuration.{ConfigCaching, ConfigFlag}

object CAPFConfiguration {

  object DebugPhysicalOperators extends ConfigFlag("capf.debugPhysicalOperators") with ConfigCaching[Boolean]

}
