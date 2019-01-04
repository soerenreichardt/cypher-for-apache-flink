/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.flink.test.support.capf

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row
import org.opencypher.flink.api.CAPFSession
import org.opencypher.flink.api.io.{CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.api.io.GraphEntity.sourceIdKey
import org.opencypher.flink.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.flink.schema.CAPFSchema._
import org.opencypher.flink.impl.convert.FlinkConversions._
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.impl.graph.ScanGraph
import org.opencypher.okapi.testing.propertygraph.InMemoryTestGraph

object CAPFScanGraphFactory extends CAPFTestGraphFactory {

  val tableEntityKey = s"___$sourceIdKey"

  override def apply(propertyGraph: InMemoryTestGraph)(implicit capf: CAPFSession): RelationalCypherGraph[FlinkTable]= {
    val schema = computeSchema(propertyGraph).asCapf

    val nodeScans = schema.labelCombinations.combos.map { labels =>
      val propKeys = schema.nodePropertyKeys(labels)

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
      val propKeys = schema.relationshipPropertyKeys(relType)

      val header = Seq(tableEntityKey, sourceStartNodeKey, sourceEndNodeKey) ++ propKeys.keys
      val rows = propertyGraph.relationships
        .filter(_.relType == relType)
        .map { rel =>
          val propertyValues = propKeys.map(key => rel.properties.unwrap.getOrElse(key._1, null))
          Row.of((Seq(rel.id, rel.startId, rel.endId) ++ propertyValues).map(v => v.asInstanceOf[AnyRef]): _*)
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

    new ScanGraph(nodeScans.toSeq ++ relScans, schema,  Set(0))
  }

  override def name: String ="CAPFScanGraphFactory"

}
