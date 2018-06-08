package org.opencypher.flink.api.io.metadata

import upickle.default._

object CAPFGraphMetaData {
  implicit def rw: ReadWriter[CAPFGraphMetaData] = macroRW

  def fromJson(jsonString: String): CAPFGraphMetaData =
    upickle.default.read[CAPFGraphMetaData](jsonString)
}

case class CAPFGraphMetaData(tableStorageFormat: String, tags: Set[Int] = Set(0)) {

  def toJson: String =
    upickle.default.write[CAPFGraphMetaData](this, indent = 4)
}
