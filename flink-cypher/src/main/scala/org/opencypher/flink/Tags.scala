package org.opencypher.flink

import org.apache.flink.table.api.Types
import org.apache.flink.table.expressions.{Expression, Literal}
import org.opencypher.okapi.impl.exception.IllegalStateException

object Tags {

  // Long representation using 64 bits containing graph tag and entity identifier
  val totalBits: Int = 64

  // Number of Bits used to store entity identifier
  val idBits: Int = 54

  val tagBits = totalBits - idBits

  // 1023 with 10 tag bits
  val maxTag: Int = (1 << tagBits) - 1

  // All possible tags, 1024 entries with 10 tag bits.
  val allTags: Set[Int] = (0 to maxTag).toSet

  // Mask to extract graph tag
  val tagMask: Long = -1L << idBits

  val invertedTagMask: Long = ~tagMask

  val invertedTagMaskLit: Expression = Literal(invertedTagMask, Types.LONG)

  /**
    * Returns a free tag given a set of used tags.
    */
  def pickFreeTag(usedTags: Set[Int]): Int = {
    if (usedTags.isEmpty) {
      0
    } else {
      val maxUsed = usedTags.max
      if (maxUsed < maxTag) {
        maxUsed + 1
      } else {
        val availableTags = allTags -- usedTags
        if (availableTags.nonEmpty) {
          availableTags.min
        } else {
          throw IllegalStateException("Could not complete this operation, ran out of tag space.")
        }
      }
    }
  }

}
