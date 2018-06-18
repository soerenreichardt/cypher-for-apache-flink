package org.opencypher.flink.test.support.capf

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row
import org.opencypher.flink.api.io.{CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.api.io.GraphEntity.sourceIdKey
import org.opencypher.flink.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.flink.schema.CAPFSchema._
import org.opencypher.flink.impl.{CAPFGraph, CAPFScanGraph, CAPFSession}
import org.opencypher.flink.impl.convert.FlinkConversions._
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.testing.propertygraph.InMemoryTestGraph

object CAPFScanGraphFactory extends CAPFTestGraphFactory {

  val tableEntityKey = s"___$sourceIdKey"

  override def apply(propertyGraph: InMemoryTestGraph)(implicit capf: CAPFSession): CAPFGraph = {
    val schema = computeSchema(propertyGraph).asCapf

    val nodeScans = schema.labelCombinations.combos.map { labels =>
      val propKeys = schema.nodeKeys(labels)

      val header = Seq(tableEntityKey) ++ propKeys.keys
      val rows = propertyGraph.nodes
        .filter(_.labels == labels)
        .map { node =>
          val propertyValues = propKeys.map(key =>
            node.properties.unwrap.getOrElse(key._1, null)
          )
          Row.of((Seq(node.id) ++ propertyValues).map(v => v.asInstanceOf[AnyRef]): _*)
        }

      implicit val nodeTypeInfo = new RowTypeInfo(
        (Seq(Types.LONG) ++ propKeys.map(propKey => propKey._2.getFlinkType)).toArray,
        header.toArray
      )

      val rowDataSet = capf.env.fromCollection(rows)
      val records = capf.tableEnv.fromDataSet(rowDataSet)

      CAPFNodeTable.fromMapping(NodeMapping
        .on(tableEntityKey)
        .withImpliedLabels(labels.toSeq: _*)
        .withPropertyKeys(propKeys.keys.toSeq: _*), records)
    }

    val relScans = schema.relationshipTypes.map { relType =>
      val propKeys = schema.relationshipKeys(relType)

      val header = Seq(tableEntityKey, sourceStartNodeKey, sourceEndNodeKey) ++ propKeys.keys
      val rows = propertyGraph.relationships
        .filter(_.relType == relType)
        .map { rel =>
          val propertyValues = propKeys.map(key => rel.properties.unwrap.getOrElse(key._1, null))
          Row.of((Seq(rel.id, rel.source, rel.target) ++ propertyValues).map(v => v.asInstanceOf[AnyRef]): _*)
        }

      implicit val relTypeInfo = new RowTypeInfo(
        (Seq(Types.LONG, Types.LONG, Types.LONG) ++ propKeys.map(propKey => propKey._2.getFlinkType)).toArray,
        header.toArray
      )
      val rowDataSet = capf.env.fromCollection(rows)
      val records = capf.tableEnv.fromDataSet(rowDataSet)

      CAPFRelationshipTable.fromMapping(RelationshipMapping
        .on(tableEntityKey)
        .from(sourceStartNodeKey)
        .to(sourceEndNodeKey)
        .relType(relType)
        .withPropertyKeys(propKeys.keys.toSeq: _*), records)
    }

    new CAPFScanGraph(nodeScans.toSeq ++ relScans, schema,  Set(0))
  }

  override def name: String ="CAPFScanGraphFactory"

}
