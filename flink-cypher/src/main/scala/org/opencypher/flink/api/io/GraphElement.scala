package org.opencypher.flink.api.io

import org.opencypher.okapi.api.graph.{SourceEndNodeKey, SourceIdKey, SourceStartNodeKey}

import scala.annotation.StaticAnnotation

sealed trait GraphElement extends Product {
  def id: Long
}

object GraphElement {
  val sourceIdKey: String = SourceIdKey.name
  val nodeSourceIdKey: String = s"node_${SourceIdKey.name}"
  val relationshipSourceIdKey: String = s"relationship_${SourceIdKey.name}"
}

/**
 * If a node has no label annotation, then the class name is used as its label.
 * If a `Labels` annotation, for example `@Labels("Person", "Mammal")`, is present,
 * then the labels from that annotation are used instead.
 */
trait Node extends GraphElement

object Relationship {
  val sourceStartNodeKey: String = SourceStartNodeKey.name

  val sourceEndNodeKey: String = SourceEndNodeKey.name

  val nonPropertyAttributes: Set[String] = Set(GraphElement.sourceIdKey, sourceStartNodeKey, sourceEndNodeKey)
}

/**
 * If a relationship has no type annotation, then the class name in upper case is used as its type.
 * If a `Type` annotation, for example `@RelationshipType("FRIEND_OF")` is present,
 * then the type from that annotation is used instead.
 */
trait Relationship extends GraphElement {
  def source: Long

  def target: Long
}

/**
 * Annotation to use when mapping a case class to a node with more than one label, or a label different to the class name.
 *
 * {{{
 *   @ Labels("Person", "Employee")
 *   case class Employee(id: Long, name: String, salary: Double)
 * }}}
 *
 * @param labels the labels that the node has.
 */
case class Labels(labels: String*) extends StaticAnnotation

/**
 * Annotation to use when mapping a case class to a relationship with a different relationship type to the class name.
 *
 * {{{
 *   @ RelationshipType("FRIEND_OF")
 *   case class Friend(id: Long, src: Long, dst: Long, since: Int)
 * }}}
 *
 * @param relType the relationship type that the relationship has.
 */
case class RelationshipType(relType: String) extends StaticAnnotation
