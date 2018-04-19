package org.opencypher.flink.test

import java.time.LocalDate

import org.scalatest._

import scala.collection.mutable

class Test extends FlatSpec with Matchers {

  "A Stack" should "pop values in last-in-first-out order" in {
    val d = java.sql.Date.valueOf(LocalDate.now())
    val stack = new mutable.Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.pop() should be (2)
    stack.pop() should be (1)
  }

  it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = new mutable.Stack[Int]
    a [NoSuchElementException] should be thrownBy {
      emptyStack.pop()
    }
  }

}
