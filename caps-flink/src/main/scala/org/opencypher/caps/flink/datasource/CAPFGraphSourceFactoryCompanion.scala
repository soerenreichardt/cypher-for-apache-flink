package org.opencypher.caps.flink.datasource

class CAPFGraphSourceFactoryCompanion(val defaultScheme: String, additionalSchemes: String*) {
  val supportedSchemes: Set[String] = additionalSchemes.toSet + defaultScheme
}
