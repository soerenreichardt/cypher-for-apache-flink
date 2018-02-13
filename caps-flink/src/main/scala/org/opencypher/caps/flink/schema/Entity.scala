package org.opencypher.caps.flink.schema

import scala.annotation.StaticAnnotation

case class Labels(labels: String*) extends StaticAnnotation

case class RelationshipType(relType: String) extends StaticAnnotation

object Entity {
  private[schema] val sourceIdKey = "id"
}

sealed trait Entity extends Product {
  def id: Long
}

/**
  * If a node has no label annotation, then the class name is used as its label.
  * If a `Labels` annotation, for example `@Labels("Person", "Mammal")`, is present,
  * then the labels from that annotation are used instead.
  */
trait Node extends Entity

object Relationship {
  private[schema] val sourceStartNodeKey = "source"

  private[schema] val sourceEndNodeKey = "target"

  private[schema] val nonPropertyAttributes =
    Set(Entity.sourceIdKey, sourceStartNodeKey, sourceEndNodeKey)
}

/**
  * If a relationship has no type annotation, then the class name in all caps is used as its type.
  * If a `Type` annotation, for example `@RelationshipType("FRIEND_OF")` is present,
  * then the type from that annotation is used instead.
  */
trait Relationship extends Entity {
  def source: Long

  def target: Long
}
