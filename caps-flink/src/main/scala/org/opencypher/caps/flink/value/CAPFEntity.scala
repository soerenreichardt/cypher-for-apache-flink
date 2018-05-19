package org.opencypher.caps.flink.value

import org.opencypher.caps.api.value.CypherValue.{CypherMap, CypherNode, CypherRelationship}

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
   override val source: Long,
   override val target: Long,
   override val relType: String,
   override val properties: CypherMap = CypherMap.empty) extends CypherRelationship[Long] {

  override type I = CAPFRelationship

  override def copy(id: Long = id, source: Long = source, target: Long = target, relType: String = relType, properties: CypherMap = properties) = {
    CAPFRelationship(id, source, target, relType, properties).asInstanceOf[this.type]
  }
}
