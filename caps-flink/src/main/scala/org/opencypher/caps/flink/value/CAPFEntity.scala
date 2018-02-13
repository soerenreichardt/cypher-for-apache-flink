package org.opencypher.caps.flink.value

import org.opencypher.caps.api.value.CypherValue.{CypherMap, CypherNode, CypherRelationship}

case class CAPFNode(
   override val id: Long,
   override val labels: Set[String] = Set.empty,
   override val properties: CypherMap = CypherMap.empty) extends CypherNode[Long]

case class CAPFRelationship (
   override val id: Long,
   override val source: Long,
   override val target: Long,
   override val relType: String,
   override val properties: CypherMap = CypherMap.empty) extends CypherRelationship[Long]
