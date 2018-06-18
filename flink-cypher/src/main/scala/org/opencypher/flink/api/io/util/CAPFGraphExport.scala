package org.opencypher.flink.api.io.util

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions.{ResolvedFieldReference, UnresolvedFieldReference}
import org.opencypher.flink.impl.convert.FlinkConversions._
import org.opencypher.flink._
import org.opencypher.flink.api.io.{GraphEntity, Relationship}
import org.opencypher.flink.impl.CAPFGraph
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.impl.util.StringEncodingUtilities._

object CAPFGraphExport {

  implicit class CanonicalTableFlinkSchema(val schema: Schema) extends AnyVal {

    def canonicalNodeTableSchema(labels: Set[String]): Seq[ResolvedFieldReference] = {
      val id = ResolvedFieldReference(GraphEntity.sourceIdKey, Types.LONG)
      val properties = schema.nodeKeys(labels).toSeq.sortBy(_._1).map { case (propertyName, cypherType) =>
        ResolvedFieldReference(propertyName.toPropertyColumnName, cypherType.getFlinkType)
      }
      Seq(id) ++ properties
    }

    def canonicalRelTableSchema(relType: String): Seq[ResolvedFieldReference] = {
      val id = ResolvedFieldReference(GraphEntity.sourceIdKey, Types.LONG)
      val sourceId = ResolvedFieldReference(Relationship.sourceStartNodeKey, Types.LONG)
      val targetId = ResolvedFieldReference(Relationship.sourceEndNodeKey, Types.LONG)
      val properties = schema.relationshipKeys(relType).toSeq.sortBy(_._1).map { case (propertyName, cypherType) =>
        ResolvedFieldReference(propertyName.toPropertyColumnName, cypherType.getFlinkType)
      }
      Seq(id, sourceId, targetId) ++ properties
    }
  }

  implicit class CanonicalTableExport(graph: CAPFGraph) {

    def canonicalNodeTable(labels: Set[String]): Table = {
      val varName = "n"
      val nodeRecords = graph.nodesWithExactLabels(varName, labels)

      val idRenaming = varName -> GraphEntity.sourceIdKey
      val properties = nodeRecords.header.propertiesFor(Var(varName)())
      val propertyRenamings = properties.map { p => nodeRecords.header.column(p) -> p.key.name.toRelTypeColumnName }

      val selectColumnsExprs = (idRenaming :: propertyRenamings.toList.sorted).map {
        case (oldName, newName) => UnresolvedFieldReference(oldName) as Symbol(newName)
      }

      nodeRecords.table.select(selectColumnsExprs: _*)
    }

    def canonicalRelationshipTable(relType: String): Table = {
      val varName = "r"
      val relCypherType = CTRelationship(relType)
      val v = Var(varName)(relCypherType)

      val relRecords = graph.relationships(varName, relCypherType)
      val header = relRecords.header

      val idRenaming = varName -> GraphEntity.sourceIdKey
      val sourceIdRenaming = header.column(header.startNodeFor(v)) -> Relationship.sourceStartNodeKey
      val targetIdRenaming = header.column(header.endNodeFor(v)) -> Relationship.sourceEndNodeKey
      val properties = header.propertiesFor(Var(varName)())
      val propertyRenamings = properties.map { p => relRecords.header.column(p) -> p.key.name.toPropertyColumnName }

      val selectColumnsExprs = (idRenaming :: sourceIdRenaming :: targetIdRenaming :: propertyRenamings.toList.sorted).map {
        case (oldName, newName) => UnresolvedFieldReference(oldName) as Symbol(newName)
      }

      relRecords.table.select(selectColumnsExprs: _*)
    }
  }

}
