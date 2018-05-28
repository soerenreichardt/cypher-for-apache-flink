package org.opencypher.flink

object TagSupport {

  def computeRetaggings[GraphKey](
    graphs: Map[GraphKey, Set[Int]],
    fixedRetaggings: Map[GraphKey, Map[Int, Int]] = Map.empty[GraphKey, Map[Int, Int]]
  ): Map[GraphKey, Map[Int, Int]] = {
    val graphsToRetag = graphs.filterNot { case (qgn, _) => fixedRetaggings.contains(qgn) }
    val usedTags = fixedRetaggings.values.flatMap(_.values).toSet
    val (result, _) = graphsToRetag.foldLeft((fixedRetaggings, usedTags)) {
      case ((graphReplacements, previousTags), (graphId, rightTags)) =>

        val replacements = previousTags.replacementsFor(rightTags)
        val updatedRightTags = rightTags.replaceWith(replacements)

        val updatedPreviousTags = previousTags ++ updatedRightTags
        val updatedGraphReplacements = graphReplacements.updated(graphId, replacements)

        updatedGraphReplacements -> updatedPreviousTags
    }
    result
  }

  implicit class TagSet(val lhsTags: Set[Int]) extends AnyVal {

    def replacementsFor(rhsTags: Set[Int]): Map[Int, Int] = {
      if (lhsTags.isEmpty && rhsTags.isEmpty) {
        Map.empty
      } else if (lhsTags.isEmpty) {
        rhsTags.zip(rhsTags).toMap
      } else if (rhsTags.isEmpty) {
        lhsTags.zip(lhsTags).toMap
      } else {
        val maxUsedTag = Math.max(lhsTags.max, rhsTags.max)
        val nextTag = maxUsedTag + 1
        val conflicts = lhsTags intersect rhsTags

        val conflictMap = conflicts.zip(nextTag until nextTag + conflicts.size).toMap

        val nonConflicts = rhsTags -- conflicts
        val identityMap = nonConflicts.zip(nonConflicts).toMap

        conflictMap ++ identityMap
      }
    }

    def replaceWith(replacements: Map[Int, Int]): Set[Int] =
      lhsTags.map(t => replacements.getOrElse(t, identity(t)))

  }

}
