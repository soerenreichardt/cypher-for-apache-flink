package org.opencypher.flink.api.io.json

import org.opencypher.flink.api.io.AbstractDataSource
import org.opencypher.flink.api.io.metadata.CAPFGraphMetaData
import org.opencypher.flink.schema.CAPFSchema
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.Schema

trait JsonSerialization {
  self: AbstractDataSource =>

  import CAPFSchema._

  protected def readJsonSchema(graphName: GraphName): String

  protected def writeJsonSchema(graphName: GraphName, schema: String): Unit

  protected def readJsonCAPFGraphMetaData(graphName: GraphName): String

  protected def writeJsonCAPFGraphMetaData(graphName: GraphName, capfGraphMetaData: String): Unit

  override protected def readSchema(graphName: GraphName): CAPFSchema = {
    Schema.fromJson(readJsonSchema(graphName)).asCapf
  }

  override protected def writeSchema(graphName: GraphName, schema: CAPFSchema): Unit = {
    writeJsonSchema(graphName, schema.schema.toJson)
  }

  override protected def readCAPFGraphMetaData(graphName: GraphName): CAPFGraphMetaData = {
    CAPFGraphMetaData.fromJson(readJsonCAPFGraphMetaData(graphName))
  }

  override protected def writeCAPFGraphMetaData(graphName: GraphName, capfGraphMetaData: CAPFGraphMetaData): Unit = {
    writeJsonCAPFGraphMetaData(graphName, capfGraphMetaData.toJson)
  }

}
