package org.opencypher.flink.test.utils

import org.opencypher.flink.api.value.{CAPFNode, CAPFRelationship}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.Bag.Bag

object BagHelpers {

  implicit class BagOps(val bag: Bag[CypherMap]) {
    def nodeValuesWithoutIds: Bag[(Set[String], CypherMap)] = for {
      (map, count) <- bag
      value <- map.value.values
      node = value.cast[CAPFNode]
    } yield (node.labels, node.properties) -> count

    def relValuesWithoutIds: Bag[(String, CypherMap)] = for {
      (map, count) <- bag
      value <- map.value.values
      rel = value.cast[CAPFRelationship]
    } yield (rel.relType, rel.properties) -> count
  }
}
