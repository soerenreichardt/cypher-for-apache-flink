package org.opencypher.flink.api.io

import org.apache.flink.table.api.{Table, TableSchema}
import org.apache.flink.table.expressions.ResolvedFieldReference
import org.opencypher.flink.api.io.metadata.CAPFGraphMetaData
import org.opencypher.flink._
import org.opencypher.flink.api.io.util.CAPFGraphExport._
import org.opencypher.flink.impl.CAPFConverters._
import org.opencypher.flink.impl.{CAPFGraph, CAPFSession}
import org.opencypher.flink.impl.io.CAPFPropertyGraphDataSource
import org.opencypher.flink.schema.CAPFSchema
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.types.CTInteger
import org.opencypher.okapi.impl.exception.{GraphAlreadyExistsException, GraphNotFoundException}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._

abstract class AbstractDataSource(implicit val session: CAPFSession) extends CAPFPropertyGraphDataSource {

  def tableStorageFormat: String

  protected var schemaCache: Map[GraphName, CAPFSchema] = Map.empty

  protected var graphNameCache: Set[GraphName] = listGraphNames.map(GraphName).toSet

  protected def listGraphNames: List[String]

  protected def deleteGraph(graphName: GraphName): Unit

  protected def readSchema(graphName: GraphName): CAPFSchema

  protected def writeSchema(graphName: GraphName, schema: CAPFSchema): Unit

  protected def readCAPFGraphMetaData(graphName: GraphName): CAPFGraphMetaData

  protected def writeCAPFGraphMetaData(graphName: GraphName, capfGraphMetaData: CAPFGraphMetaData): Unit

  protected def readNodeTable(graphName: GraphName, tableStorageFormat: String, labels: Set[String], tableSchema: Seq[ResolvedFieldReference]): Table

  protected def writeNodeTable(graphName: GraphName, tableStorageFormat: String, labels: Set[String], table: Table): Unit

  protected def readRelationshipTable(graphName: GraphName, relKey: String, tableSchema: Seq[ResolvedFieldReference]): Table

  protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: Table): Unit

  override def graphNames: Set[GraphName] = graphNameCache

  override def hasGraph(graphName: GraphName): Boolean = graphNameCache.contains(graphName)

  override def delete(graphName: GraphName): Unit = {
    schemaCache -= graphName
    graphNameCache -= graphName
    deleteGraph(graphName)
  }

  override def graph(graphName: GraphName): PropertyGraph = {
    if (!hasGraph(graphName)) {
      throw GraphNotFoundException(s"Graph with name '$graphName'")
    } else {
      val capfSchema: CAPFSchema = schema(graphName).get
      val capfMetaData: CAPFGraphMetaData = readCAPFGraphMetaData(graphName)
      val nodeTables = capfSchema.allLabelCombinations.map { combo =>
        val propertyColsWithCypherType = capfSchema.keysFor(Set(combo)).map {
          case (key, cypherType) => key.toPropertyColumnName -> cypherType
        }

        val columnsWithCypherType = propertyColsWithCypherType + (GraphEntity.sourceIdKey -> CTInteger)
        val table = readNodeTable(graphName, capfMetaData.tableStorageFormat, combo, capfSchema.canonicalNodeTableSchema(combo))
        CAPFNodeTable(combo, table)
      }

      val relTables = capfSchema.relationshipTypes.map { relType =>
        val propertyColsWithCypherType = capfSchema.relationshipKeys(relType).map {
          case (key, cypherType) => key.toPropertyColumnName -> cypherType
        }

        val table = readRelationshipTable(graphName, relType, capfSchema.canonicalRelTableSchema(relType))
        CAPFRelationshipTable(relType, table)
      }
      CAPFGraph.create(capfMetaData.tags, nodeTables.head, (nodeTables.tail ++ relTables).toSeq: _*)
    }
  }

  override def schema(graphName: GraphName): Option[CAPFSchema] = {
    if (schemaCache.contains(graphName)) {
      schemaCache.get(graphName)
    } else {
      val s = readSchema(graphName)
      schemaCache += graphName -> s
      Some(s)
    }
  }

  override def store(graphName: GraphName, graph: PropertyGraph): Unit = {
    if (hasGraph(graphName)) {
      throw GraphAlreadyExistsException(s"A graph with name $graphName is already stored in this graph data source.")
    }

    val capfGraph = graph.asCapf
    val schema = capfGraph.schema
    schemaCache += graphName -> schema
    graphNameCache += graphName
    writeCAPFGraphMetaData(graphName, CAPFGraphMetaData(tableStorageFormat, capfGraph.tags))
    writeSchema(graphName, schema)

    schema.labelCombinations.combos.foreach { combo =>
      writeNodeTable(graphName, tableStorageFormat, combo, capfGraph.canonicalNodeTable(combo))
    }

    schema.relationshipTypes.foreach { relType =>
      writeRelationshipTable(graphName, relType, capfGraph.canonicalRelationshipTable(relType))
    }
  }

}