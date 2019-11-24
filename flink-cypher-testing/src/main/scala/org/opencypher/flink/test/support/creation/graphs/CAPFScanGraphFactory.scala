package org.opencypher.flink.test.support.creation.graphs

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.expressions.{ResolvedFieldReference, UnresolvedFieldReference}
import org.apache.flink.types.Row
import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.api.io.CAPFElementTable
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.flink.test.support.EntityTableCreationSupport
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.{CypherValue, Element}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException}
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.impl.graph.ScanGraph
import org.opencypher.okapi.testing.propertygraph.{InMemoryTestGraph, InMemoryTestNode, InMemoryTestRelationship}
import org.opencypher.flink.schema.CAPFSchema._
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.flink.impl.convert.FlinkConversions._

object CAPFScanGraphFactory extends TestGraphFactory with EntityTableCreationSupport {

  override def apply(propertyGraph: InMemoryTestGraph, additionalPatterns: Seq[Pattern])
    (implicit capf: CAPFSession): RelationalCypherGraph[FlinkTable] = {

    val schema = computeSchema(propertyGraph).asCapf

    val nodePatterns = schema.labelCombinations.combos.map(labels => NodePattern(CTNode(labels)))
    val relPatterns = schema.relationshipTypes.map(typ => RelationshipPattern(CTRelationship(typ)))

    val scans = (nodePatterns ++ relPatterns ++ additionalPatterns).map { pattern =>
      val data = extractEmbeddings(pattern, propertyGraph, schema)
      createEntityTable(pattern, data, schema)
    }

    new ScanGraph(scans.toSeq, schema)
  }

  override def name: String = "CAPFScanGraphFactory"

  private def extractEmbeddings(pattern: Pattern, graph: InMemoryTestGraph, schema: PropertyGraphSchema)
    (implicit capf: CAPFSession): Seq[Map[PatternElement, Element[Long]]] = {

    val candidates = pattern.elements.map { entity =>
      entity.cypherType match {
        case CTNode(labels, _) =>
          entity -> graph.nodes.filter(_.labels == labels)
        case CTRelationship(types, _) =>
          entity -> graph.relationships.filter(rel => types.contains(rel.relType))
        case other => throw IllegalArgumentException("Node or Relationship type", other)
      }
    }.toMap

    val unitEmbedding = Seq(
      Map.empty[PatternElement, Element[Long]]
    )
    val initialEmbeddings = pattern.elements.foldLeft(unitEmbedding) {
      case (acc, entity) =>
        val entityCandidates = candidates(entity)

        for {
          row <- acc
          entityCandidate <- entityCandidates
        } yield row.updated(entity, entityCandidate)
    }

    pattern.topology.foldLeft(initialEmbeddings) {
      case (acc, (relEntity, connection)) =>
        connection match {
          case Connection(Some(sourceNode), None, _) => acc.filter { row =>
            row(sourceNode).id == row(relEntity).asInstanceOf[InMemoryTestRelationship].startId
          }

          case Connection(None, Some(targetEntity), _) => acc.filter { row =>
            row(targetEntity).id == row(relEntity).asInstanceOf[InMemoryTestRelationship].endId
          }

          case Connection(Some(sourceNode), Some(targetEntity), _) => acc.filter { row =>
            val rel = row(relEntity).asInstanceOf[InMemoryTestRelationship]
            row(sourceNode).id == rel.startId && row(targetEntity).id == rel.endId
          }

          case Connection(None, None, _) => throw IllegalStateException("Connection without source or target node")
        }
    }
  }

  private def createEntityTable(
    pattern: Pattern,
    embeddings: Seq[Map[PatternElement, Element[Long]]],
    schema: PropertyGraphSchema
  )(implicit capf: CAPFSession): CAPFElementTable = {

    val unitData: Seq[Seq[Any]] = Seq(embeddings.indices.map(_ => Seq.empty[Any]): _*)

    val (columns, data) = pattern.elements.foldLeft(Seq.empty[ResolvedFieldReference] -> unitData) {
      case ((accFieldRefs, accData), entity) =>

        entity.cypherType match {
          case CTNode(labels, _) =>
            val propertyKeys = schema.nodePropertyKeys(labels)
            val propertyFields = getPropertyFields(entity, propertyKeys)

            val nodeData = embeddings.map { embedding =>
              val node = embedding(entity).asInstanceOf[InMemoryTestNode]

              val propertyValues = propertyKeys.keySet.toSeq.map(p => node.properties.get(p).map(toFlinkValue).orNull)
              Seq(node.id) ++ propertyValues
            }

            val newData = accData.zip(nodeData).map { case (l, r) => l ++ r }
            val newColumns = accFieldRefs ++ Seq(ResolvedFieldReference(s"${entity.name.encodeSpecialCharacters}_id", Types.LONG)) ++ propertyFields

            newColumns -> newData

          case CTRelationship(types, _) =>
            val propertyKeys = schema.relationshipPropertyKeys(types.head)
            val propertyFields = getPropertyFields(entity, propertyKeys)

            val relData = embeddings.map { embedding =>
              val rel = embedding(entity).asInstanceOf[InMemoryTestRelationship]
              val propertyValues = propertyKeys.keySet.toSeq.map(p => rel.properties.get(p).map(toFlinkValue).orNull)
              Seq(rel.id, rel.startId, rel.endId) ++ propertyValues
            }

            val newData = accData.zip(relData).map { case (l, r) => l ++ r }
            val newColumns = accFieldRefs ++
              Seq(
                ResolvedFieldReference(s"${entity.name.encodeSpecialCharacters}_id", Types.LONG),
                ResolvedFieldReference(s"${entity.name.encodeSpecialCharacters}_source", Types.LONG),
                ResolvedFieldReference(s"${entity.name.encodeSpecialCharacters}_target", Types.LONG)
              ) ++
              propertyFields

            newColumns -> newData

          case other => throw IllegalArgumentException("Node or Relationship type", other)
        }
    }

    val dataAsRows = data.map { row =>
      Row.of(row.map(_.asInstanceOf[AnyRef]): _*)
    }

    implicit val rowTypeInfo = new RowTypeInfo(columns.map(_.resultType): _*)

    val table = capf.tableEnv.fromDataSet(
      capf.env.fromCollection(
        dataAsRows
      ),
      columns.map(ref => UnresolvedFieldReference(ref.name)): _*
    )

    constructEntityTable(pattern, table)
  }

  protected def getPropertyFields(patternElement: PatternElement, propKeys: PropertyKeys): Seq[ResolvedFieldReference] = {
    propKeys.foldLeft(Seq.empty[ResolvedFieldReference]) {
      case (fields, key) => fields :+ ResolvedFieldReference(s"${patternElement.name}_${key._1.encodeSpecialCharacters}_property", key._2.getFlinkType)
    }
  }

  private def toFlinkValue(v: CypherValue): Any = {
    v.getValue match {
      case Some(l: List[_]) => l.collect { case c: CypherValue => toFlinkValue(c) }
      case Some(other) => other
      case None => null
    }
  }
//    val nodeScans = schema.labelCombinations.combos.map { labels =>
//      val propKeys = schema.nodePropertyKeys(labels)
//
//      val header = Seq(tableEntityKey) ++ propKeys.keys
//      val rows = propertyGraph.nodes
//        .filter(_.labels == labels)
//        .map { node =>
//          val propertyValues = propKeys.map(key =>
//            node.properties.unwrap.getOrElse(key._1, null)CAPFSchema
//          )
//          Row.of((Seq(node.id) ++ propertyValues).map(v => v.asInstanceOf[AnyRef]): _*)
//        }
//
//      implicit val nodeTypeInfo = new RowTypeInfo(
//        (Seq(Types.LONG) ++ propKeys.map(propKey => propKey._2.getFlinkType)).toArray,
//        header.toArray
//      )
//
//      val rowDataSet = capf.env.fromCollection(rows)
//      val records = capf.tableEnv.fromDataSet(rowDataSet)
//
//      val mapping = NodeMappingBuilder
//        .on(tableEntityKey)
//        .withImpliedLabels(labels.toSeq: _*)
//        .withPropertyKeys(propKeys.keys.toSeq: _*)
//        .build
//
//      CAPFEntityTable.create(mapping, records)
//    }
//
//    val relScans = schema.relationshipTypes.map { relType =>
//      val propKeys = schema.relationshipPropertyKeys(relType)
//
//      val header = Seq(tableEntityKey, sourceStartNodeKey, sourceEndNodeKey) ++ propKeys.keys
//      val rows = propertyGraph.relationships
//        .filter(_.relType == relType)
//        .map { rel =>
//          val propertyValues = propKeys.map(key => rel.properties.unwrap.getOrElse(key._1, null))
//          Row.of((Seq(rel.id, rel.startId, rel.endId) ++ propertyValues).map(v => v.asInstanceOf[AnyRef]): _*)
//        }
//
//      implicit val relTypeInfo = new RowTypeInfo(
//        (Seq(Types.LONG, Types.LONG, Types.LONG) ++ propKeys.map(propKey => propKey._2.getFlinkType)).toArray,
//        header.toArray
//      )
//      val rowDataSet = capf.env.fromCollection(rows)
//      val records = capf.tableEnv.fromDataSet(rowDataSet)
//
//      val mapping = RelationshipMappingBuilder
//        .on(tableEntityKey)
//        .from(sourceStartNodeKey)
//        .to(sourceEndNodeKey)
//        .relType(relType)
//        .withPropertyKeys(propKeys.keys.toSeq: _*)
//        .build
//      CAPFEntityTable.create(mapping, records)
//    }
//
//    new ScanGraph(nodeScans.toSeq ++ relScans, schema)
//  }

}
