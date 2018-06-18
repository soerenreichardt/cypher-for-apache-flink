package org.opencypher.flink.api.io

import scala.annotation.StaticAnnotation

sealed trait GraphEntity extends Product {
  def id: Long
}

object GraphEntity {
  val sourceIdKey = "id"
}

trait Node extends GraphEntity

object Relationship {
  val sourceStartNodeKey = "source"

  val sourceEndNodeKey = "target"

  val nonPropertyAttributes = Set(GraphEntity.sourceIdKey, sourceStartNodeKey, sourceEndNodeKey)
}

trait Relationship extends GraphEntity {
  def source: Long

  def target: Long
}

case class Labels(labels: String*) extends StaticAnnotation

case class RelationshipType(relType: String) extends StaticAnnotation