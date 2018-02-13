package org.opencypher.caps.flink

import org.opencypher.caps.flink.schema.{Labels, Node, Relationship, RelationshipType}

import scala.annotation.StaticAnnotation
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox


private[caps] object Annotation {
  def labels[E <: Node: TypeTag]: Set[String] = {
    get[Labels, E] match {
      case Some(ls) => ls.labels.toSet
      case None     => Set(runtimeClass[E].getSimpleName)
    }
  }

  def relType[E <: Relationship: TypeTag]: String = {
    get[RelationshipType, E] match {
      case Some(RelationshipType(tpe)) => tpe
      case None                        => runtimeClass[E].getSimpleName.toUpperCase
    }
  }

  def get[A <: StaticAnnotation: TypeTag, E: TypeTag]: Option[A] = {
    val maybeAnnotation = staticClass[E].annotations.find(_.tree.tpe =:= typeOf[A])
    maybeAnnotation.map { annotation =>
      val tb = typeTag[E].mirror.mkToolBox()
      val instance = tb.eval(tb.untypecheck(annotation.tree)).asInstanceOf[A]
      instance
    }
  }

  private def runtimeClass[E: TypeTag]: Class[E] = {
    val tag = typeTag[E]
    val mirror = tag.mirror
    val runtimeClass = mirror.runtimeClass(tag.tpe.typeSymbol.asClass)
    runtimeClass.asInstanceOf[Class[E]]
  }

  private def staticClass[E: TypeTag]: ClassSymbol = {
    val mirror = typeTag[E].mirror
    mirror.staticClass(runtimeClass[E].getCanonicalName)
  }
}
