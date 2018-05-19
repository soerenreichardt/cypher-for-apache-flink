package org.opencypher.flink.physical.operators

import org.opencypher.flink.ColumnName
import org.opencypher.okapi.relational.impl.table.RecordHeader

import scala.annotation.tailrec
import scala.util.Random

object ColumnNameGenerator {
  val NAME_SIZE = 5

  @tailrec
  def generateUniqueName(header: RecordHeader): String = {
    val chars = (1 to NAME_SIZE).map(_ => Random.nextPrintableChar())
    val name = ColumnName.from(String.valueOf(chars.toArray))

    if (header.slots.map(ColumnName.of).contains(name)) generateUniqueName(header)
    else name
  }
}
