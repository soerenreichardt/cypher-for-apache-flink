package org.opencypher.flink.physical.operators

import org.apache.flink.table.expressions.{If, UnresolvedFieldReference}
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.opencypher.flink._
import org.opencypher.flink.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.flink.physical.operators.CAPFPhysicalOperator._
import org.opencypher.flink.schema.EntityTable._
import org.opencypher.flink.TableOps._
import org.opencypher.flink.TagSupport._
import org.opencypher.flink.CAPFSchema._
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.logical.impl.LogicalPatternGraph
import org.opencypher.okapi.relational.impl.physical.LeftOuterJoin
import org.opencypher.okapi.relational.impl.table.{OpaqueField, RecordHeader, RecordSlot}

private[flink] abstract class BinaryPhysicalOperator extends CAPFPhysicalOperator {

  def lhs: CAPFPhysicalOperator

  def rhs: CAPFPhysicalOperator

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = executeBinary(lhs.execute, rhs.execute)

  def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult
}

final case class Join(
  lhs: CAPFPhysicalOperator,
  rhs: CAPFPhysicalOperator,
  joinColumns: Seq[(Expr, Expr)],
  header: RecordHeader,
  joinType: String)
  extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val joinSlots = joinColumns.map {
      case (leftExpr, rightExpr) =>
        val leftRecordSlot = header.slotsFor(leftExpr)
          .headOption
          .getOrElse(throw IllegalArgumentException("Expression mapping to a single column", leftExpr))
        val rightRecordSlot = header.slotsFor(rightExpr)
          .headOption
          .getOrElse(throw IllegalArgumentException("Expression mapping to a single column", rightExpr))

        leftRecordSlot -> rightRecordSlot
    }

    val joinedRecords = joinRecords(header, joinSlots, joinType)(left.records, right.records)

    CAPFPhysicalResult(joinedRecords, left.workingGraph, left.workingGraphName)
  }

}

//final case class Union(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator)
//  extends BinaryPhysicalOperator with InheritedHeader {
//
//  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
//    val leftData = left.records.data
//    val rightData = right.records.data.select(lhs.header.slots.map(slot => UnresolvedFieldReference(ColumnName.of(slot.content))): _*)
//
//    val unionedData = leftData.union(rightData)
//    val records = CAPFRecords.verifyAndCreate(header, unionedData)(left.records.capf)
//
//    CAPFPhysicalResult(records, left.graphs ++ right.graphs)
//  }
//}

final case class CartesianProduct(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator, header: RecordHeader)
  extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val data = left.records.data
    val otherData = right.records.data
    val newData = data.cross(otherData)(left.records.capf)

    val records = CAPFRecords.verifyAndCreate(header, newData)(left.records.capf)
    CAPFPhysicalResult(records, left.workingGraph, left.workingGraphName)
  }
}

final case class ExistsSubQuery(
  lhs: CAPFPhysicalOperator,
  rhs: CAPFPhysicalOperator,
  targetField: Var,
  header: RecordHeader)
  extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val leftData = left.records.data
    val rightData = right.records.data
    val leftHeader = left.records.header
    val rightHeader = right.records.header

    val joinFields = leftHeader.fieldsAsVar.intersect(rightHeader.fieldsAsVar)

    val columnsToRemove = joinFields
      .flatMap(rightHeader.childSlots)
      .map(_.content)
      .map(ColumnName.of)
      .toSeq

    val lhsJoinSlots = joinFields.map(leftHeader.slotFor)
    val rhsJoinSlots = joinFields.map(rightHeader.slotFor)

    val joinColumnMapping = lhsJoinSlots
      .map(lhsSlot => {
        lhsSlot -> rhsJoinSlots.find(_.content == lhsSlot.content).get
      })
      .map(pair => {
        val lhsCol = ColumnName.of(pair._1)
        val rhsColName = ColumnName.of(pair._2)

        (lhsCol, rhsColName, ColumnNameGenerator.generateUniqueName(rightHeader))
      })
      .toSeq

    val reduceRhsData = joinColumnMapping
      .foldLeft(rightData)((acc, col) => acc.safeRenameColumn(col._2, col._3))
      .safeDropColumns(columnsToRemove: _*)

    val distinctRightData = reduceRhsData.distinct() // TODO: might be incorrect

    val joinCols = joinColumnMapping.map(t => t._1 -> t._3)

    val joinedRecords =
      joinTables(left.records.data, distinctRightData, header, joinCols, "left_outer")(deduplicate = true)(left.records.capf)

    val targetFieldColumnName = ColumnName.of(rightHeader.slotFor(targetField))

    val updatedJoinedRecords = joinedRecords.data
      .safeReplaceColumn(
        targetFieldColumnName,
        If(UnresolvedFieldReference(targetFieldColumnName).isNull, false, true)
      )

    CAPFPhysicalResult(CAPFRecords.verifyAndCreate(header, updatedJoinedRecords)(left.records.capf), left.workingGraph, left.workingGraphName)
  }
}

/**
  * This operator performs a left outer join between the already matched path and the optional matched pattern and
  * updates the resulting columns.
  *
  * @param lhs previous match data
  * @param rhs optional match data
  * @param header result header (lhs header + rhs header)
  */
final case class Optional(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator, header: RecordHeader)
  extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(
    implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val leftData = left.records.data
    val rightData = right.records.data
    val leftHeader = left.records.header
    val rightHeader = right.records.header

    val commonFields = leftHeader.slots.intersect(rightHeader.slots)

    val (joinSlots, otherCommonSlots) = commonFields.partition {
      case RecordSlot(_, _: OpaqueField) => true
      case RecordSlot(_, _)              => false
    }

    val joinFields = joinSlots
      .map(_.content)
      .collect { case OpaqueField(v) => v }

    val otherCommonFields = otherCommonSlots
      .map(_.content)

    val columnsToRemove = joinFields
      .flatMap(rightHeader.childSlots)
      .map(_.content)
      .union(otherCommonFields)
      .map(ColumnName.of)

    val lhsJoinSlots = joinFields.map(leftHeader.slotFor)
    val rhsJoinSlots = joinFields.map(rightHeader.slotFor)

    // Find the join pairs and introduce an alias for the right hand side
    // This is necessary to be able to deduplicate the join columns later
    val joinColumnMapping = lhsJoinSlots
      .map(lhsSlot => {
        lhsSlot -> rhsJoinSlots.find(_.content == lhsSlot.content).get
      })
      .map(pair => {
        val lhsColName = ColumnName.of(pair._1)
        val rhsColName = ColumnName.of(pair._2)

        (lhsColName, rhsColName, ColumnNameGenerator.generateUniqueName(rightHeader))
      })

    // Rename join columns on the right hand side and drop common non-join columns
    val reducedRhsData = joinColumnMapping
      .foldLeft(rightData)((acc, col) => acc.safeRenameColumn(col._2, col._3))
      .safeDropColumns(columnsToRemove: _*)

    val joinCols = joinColumnMapping.map(t => t._1 -> t._3)
    val joinedRecords =
      joinTables(left.records.data, reducedRhsData, header, joinCols, "left")(deduplicate = true)(left.records.capf)

    CAPFPhysicalResult(joinedRecords, left.workingGraph, left.workingGraphName)
  }
}

final case class TabularUnionAll(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator) extends BinaryPhysicalOperator with InheritedHeader {

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val leftData = left.records.data
    val rightData = right.records.data.select(leftData.columns.map(UnresolvedFieldReference): _*)

    val unionedData = leftData.union(rightData)
    val records = CAPFRecords.verifyAndCreate(header, unionedData)(left.records.capf)

    CAPFPhysicalResult(records, left.workingGraph, left.workingGraphName)
  }
}

final case class ConstructGraph(
  lhs: CAPFPhysicalOperator,
  rhs: CAPFPhysicalOperator,
  construct: LogicalPatternGraph)
  extends BinaryPhysicalOperator {

  override def toString: String = {
    val entities = construct.clones.keySet ++ construct.newEntities.map(_.v)
    s"ConstructGraph(on=[${construct.onGraphs.mkString(", ")}], entities=[${entities.mkString(", ")}])"
  }

  override def header: RecordHeader = RecordHeader.empty

  private def pickFreeTag(tagStrategy: Map[QualifiedGraphName, Map[Int, Int]]): Int = {
    val usedTags = tagStrategy.values.flatMap(_.values).toSet
    Tags.pickFreeTag(usedTags)
  }

  private def identityRetaggings(g: CAPFGraph): (CAPFGraph, Map[Int, Int]) = {
    g -> g.tags.zip(g.tags).toMap
  }

  override def executeBinary(left: CAPFPhysicalResult, right: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    ???
//    implicit val session: CAPFSession = left.records.capf
//
//    val onGraph = right.workingGraph
//    val unionTagStrategy: Map[QualifiedGraphName, Map[Int, Int]] = right.tagStrategy
//
//    val LogicalPatternGraph(schema, clonedVarsToInputVars, newEntities, sets, _, name) = construct
//
//    val matchGraphs: Set[QualifiedGraphName] = clonedVarsToInputVars.values.map(_.cypherType.graph.get).toSet
//    val allGraphs = unionTagStrategy.keySet ++ matchGraphs
//    val tagsForGraph: Map[QualifiedGraphName, Set[Int]] = allGraphs.map(qgn => qgn -> resolveTags(qgn)).toMap
//
//    val constructTagStrategy = computeRetaggings(tagsForGraph, unionTagStrategy)
//
//    val aliasClones = clonedVarsToInputVars.filter { case (alias, original) => alias != original }
//    val baseTable = left.records.addAliases(aliasClones)
//
//    val retaggedBaseTable = clonedVarsToInputVars.foldLeft(baseTable) { case (table, clone) =>
//      table.retagVariable(clone._1, constructTagStrategy(clone._2.cypherType.graph.get)9)
//    }
//
//    val (newEntityTags, tableWithConstructedEntities) = {
//      if (newEntities.isEmpty) {
//        Set.empty[Int] -> retaggedBaseTable
//      } else {
//        val newEntityTag = pickFreeTag(constructTagStrategy)
//        val entityTable = createEntities(newEntities, retaggedBaseTable, newEntityTag)
//        val entityTableWithProperties = sets.foldLeft(entityTable) {
//          case (table, SetPropertyItem(key, v, expr)) =>
//            constructProperty(v, key, expr, table)
//        }
//        Set(newEntityTag) -> entityTableWithProperties
//      }
//    }
//
//    val allInputVars = baseTable.header.internalHeader.fields
//    val originalVarsToKeep = clonedVarsToInputVars.keySet -- aliasClones.keySet
//    val varsToRemoveFromTable = allInputVars -- originalVarsToKeep
//    val patternGraphTable = tableWithConstructedEntities.removeVars(varsToRemoveFromTable)
//
//    val tagsUsed = constructTagStrategy.foldLeft(newEntityTags) {
//      case (tags, (qgn, remapping)) =>
//        val remappedTags = tagsFroGraph(qgn).map(remapping)
//        tags ++ remappedTags
//    }
//
//    val patternGraph = CAPFGraph.create(patternGraphTable, schema.asCapf, tagsUsed)
//
  }

}
