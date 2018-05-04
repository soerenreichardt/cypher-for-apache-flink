package org.opencypher.flink.test.support.capf

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row
import org.opencypher.flink.FlinkUtils._
import org.opencypher.flink.schema.{CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.{CAPFGraph, CAPFScanGraph, CAPFSession}
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.ir.test.support.creation.propertygraph.TestPropertyGraph

object CAPFScanGraphFactory extends CAPFTestGraphFactory {

  override def apply(propertyGraph: TestPropertyGraph)(implicit capf: CAPFSession): CAPFGraph = {
    val schema = computeSchema(propertyGraph)

    val nodeScans = schema.labelCombinations.combos.map { labels =>
      val propKeys = schema.nodeKeys(labels)

      val header = Seq("ID") ++ propKeys.keys
      val rows = propertyGraph.nodes
        .filter(_.labels == labels)
        .map { node =>
          val propertyValues = propKeys.map(key =>
            node.properties.unwrap.getOrElse(key._1, null)
          )
          Row.of((Seq(node.id) ++ propertyValues).map(v => v.asInstanceOf[AnyRef]): _*)
        }

      // TODO: maybe a row type information is necessary here
      implicit val nodeTypeInfo = new RowTypeInfo(
        (Seq(Types.LONG) ++ propKeys.map(propKey => toFlinkType(propKey._2))).toArray,
        header.toArray
      )
      val rowDataSet = capf.env.fromCollection(rows)
      val records = capf.tableEnv.fromDataSet(rowDataSet)

      CAPFNodeTable(NodeMapping
        .on("ID")
        .withImpliedLabels(labels.toSeq: _*)
        .withPropertyKeys(propKeys.keys.toSeq: _*), records)
    }

    val relScans = schema.relationshipTypes.map { relType =>
      val propKeys = schema.relationshipKeys(relType)

      val header = Seq("ID", "SRC", "DST") ++ propKeys.keys
      val rows = propertyGraph.relationships
        .filter(_.relType == relType)
        .map { rel =>
          val propertyValues = propKeys.map(key => rel.properties.unwrap.getOrElse(key._1, null))
          Row.of((Seq(rel.id, rel.source, rel.target) ++ propertyValues).map(v => v.asInstanceOf[AnyRef]): _*)
        }

      // TODO: see above

      implicit val relTypeInfo = new RowTypeInfo(
        (Seq(Types.LONG, Types.LONG, Types.LONG) ++ propKeys.map(propKey => toFlinkType(propKey._2))).toArray,
        header.toArray
      )
      val rowDataSet = capf.env.fromCollection(rows)
      val records = capf.tableEnv.fromDataSet(rowDataSet)

      CAPFRelationshipTable(RelationshipMapping
        .on("ID")
        .from("SRC")
        .to("DST")
        .relType(relType)
        .withPropertyKeys(propKeys.keys.toSeq: _*), records)
    }

    new CAPFScanGraph(nodeScans.toSeq ++ relScans, schema)
  }

  override def name: String ="CAPFScanGraphFactory"

}
