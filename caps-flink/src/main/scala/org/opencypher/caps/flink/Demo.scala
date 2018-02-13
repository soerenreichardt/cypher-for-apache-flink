package org.opencypher.caps.flink

import org.opencypher.caps.api._
import org.opencypher.caps.flink._
import org.opencypher.caps.flink.schema.{Node, Relationship}

object Demo extends App {

  CAPFSession.readFrom(DemoData.persons, DemoData.friendships)

}

object DemoData {

  case class Person(id: Long, name: String, age: Int) extends Node

  case class Friend(id: Long, source: Long, target: Long, since: String) extends Relationship

  val alice = Person(0, "Alice", 42)
  val bob = Person(1, "Bob", 23)

  val persons = List(alice, bob)
  val friendships = List(Friend(0, alice.id, bob.id, "2018"))
}
