package org.opencypher.flink.api.value

import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNode, CypherRelationship}

case class CAPFNode(
   override val id: Long,
   override val labels: Set[String] = Set.empty,
   override val properties: CypherMap = CypherMap.empty) extends CypherNode[Long] {

  override type I = CAPFNode

  override def copy(id: Long = id, labels: Set[String] = labels, properties: CypherMap = properties) = {
    CAPFNode(id, labels, properties)
  }
}

case class CAPFRelationship (
   override val id: Long,
   override val startId: Long,
   override val endId: Long,
   override val relType: String,
   override val properties: CypherMap = CypherMap.empty) extends CypherRelationship[Long] {

  override type I = CAPFRelationship

  override def copy(id: Long = id, source: Long = startId, target: Long = endId, relType: String = relType, properties: CypherMap = properties) = {
    CAPFRelationship(id, source, target, relType, properties).asInstanceOf[this.type]
  }
}
