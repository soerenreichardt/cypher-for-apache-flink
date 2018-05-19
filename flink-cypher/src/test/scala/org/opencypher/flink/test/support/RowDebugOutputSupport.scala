package org.opencypher.flink.test.support

import org.apache.flink.types.Row
import org.opencypher.okapi.ir.test.support.DebugOutputSupport

import scala.collection.Bag
import scala.collection.immutable.HashedBagConfiguration

trait RowDebugOutputSupport extends DebugOutputSupport {

  implicit class RowPrinter(bag: Bag[Row]) {
    def debug(): String = {
      val rowStrings = bag.map { row =>
        val rowAsString = (0 to row.getArity).map { i =>
          row.getField(i).asInstanceOf[Any] match {
            case null => "null"
            case s: String => s"""$s"""
            case l: Long => s"""${l}L"""
            case other => other.toString
          }
        }

        rowAsString.mkString("Row(", ", ", ")")
      }

      rowStrings.mkString("Bag(", ", \n", ")")
    }

    def printRows(): Unit = println(debug())
  }

  implicit class IterableToBagConverter(val elements: Iterable[Row]) {
    def toBag: Bag[Row] = Bag(elements.toSeq: _*)
  }

  implicit class ArrayToBagConverter(val elements: Array[Row]) {
    def toBag: Bag[Row] = Bag(elements.toSeq: _*)
  }

  implicit val m: HashedBagConfiguration[Row] = Bag.configuration.compact[Row]
  implicit val m2: HashedBagConfiguration[String] = Bag.configuration.compact[String]
}
