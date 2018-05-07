/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.okapi.relational.impl.physical

import org.opencypher.okapi.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CTVoid, CTWildcard}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.relational.impl.flat
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.physical.{PhysicalOperator, PhysicalOperatorProducer, PhysicalPlannerContext, RuntimeContext}
import org.opencypher.okapi.relational.impl.flat.FlatOperator
import org.opencypher.okapi.relational.impl.table.{ColumnName, RecordHeader}

class PhysicalPlanner[P <: PhysicalOperator[R, G, C], R <: CypherRecords, G <: PropertyGraph, C <: RuntimeContext[R, G] ](producer: PhysicalOperatorProducer[P, R, G, C])

  extends DirectCompilationStage[FlatOperator, P, PhysicalPlannerContext[R]] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext[R]): P = {

    implicit val caps: CypherSession = context.session

    flatPlan match {
      case flat.CartesianProduct(lhs, rhs, header) =>
        producer.planCartesianProduct(process(lhs), process(rhs), header)

      case flat.RemoveAliases(dependent, in, header) =>
        producer.planRemoveAliases(process(in), dependent, header)

      case flat.Select(fields, graphs: Set[String], in, header) =>
        producer.planSelect(process(in), fields.map(header.slotFor).map(_.content.key), header)

      case flat.EmptyRecords(in, header) =>
        producer.planEmptyRecords(process(in), header)

      case flat.Start(graph, _) =>
        graph match {
          case g: LogicalExternalGraph =>
            producer.planStart(context.inputRecords, g)

          case _ => throw IllegalArgumentException("a LogicalExternalGraph", graph)
        }

      case flat.SetSourceGraph(graph, in, _) =>
        graph match {
          case g: LogicalExternalGraph =>
            producer.planSetSourceGraph(process(in), g)

          case _ => throw IllegalArgumentException("a LogicalExternalGraph", graph)
        }

      case op@flat.NodeScan(v, in, header) =>
        producer.planNodeScan(process(in), op.sourceGraph, v, header)

      case op@flat.EdgeScan(e, in, header) =>
        producer.planRelationshipScan(process(in), op.sourceGraph, e, header)

      case flat.Alias(expr, alias, in, header) =>
        producer.planAlias(process(in), expr, alias, header)

      case flat.BulkAlias(exprs, aliases, in, header) =>
        producer.planBulkAlias(process(in), exprs, aliases, header)

      case flat.Unwind(list, item, in, header) =>
        producer.planUnwind(process(in), list, item, header)

      case flat.Project(expr, in, header) =>
        producer.planProject(process(in), expr, header)

      case flat.ProjectGraph(graph, in, header) =>
        graph match {
          case LogicalExternalGraph(name, qualifiedGraphName, _) =>
            producer.planProjectExternalGraph(process(in), name, qualifiedGraphName)
          case LogicalPatternGraph(name, targetSchema, GraphOfPattern(toCreate, _)) =>
            producer.planProjectPatternGraph(process(in), toCreate, name, targetSchema, header)
        }

      case flat.Aggregate(aggregations, group, in, header) =>
        producer.planAggregate(process(in), group, aggregations, header)

      case flat.Filter(expr, in, header) =>
        expr match {
          case TrueLit() =>
            process(in) // optimise away filter
          case _ =>
            producer.planFilter(process(in), expr, header)
        }

      case flat.ValueJoin(lhs, rhs, predicates, header) =>
        val joinExpressions = predicates.map(p => p.lhs -> p.rhs).toSeq
        producer.planJoin(process(lhs), process(rhs), joinExpressions, header)

      case flat.Distinct(fields, in, _) =>
        producer.planDistinct(process(in), fields)

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of just source and target and planning rels only at the physical layer
      case op@flat.Expand(source, rel, direction, target, sourceOp, targetOp, header, relHeader) =>
        val first = process(sourceOp)
        val third = process(targetOp)

        val externalGraph = sourceOp.sourceGraph match {
          case e: LogicalExternalGraph => e
          case _ => throw IllegalArgumentException("a LogicalExternalGraph", sourceOp.sourceGraph)
        }

        val startFrom = producer.planStartFromUnit(externalGraph)
        val second = producer.planRelationshipScan(startFrom, op.sourceGraph, rel, relHeader)

        def expandSource(_first: P, _second: P, _third: P, _source: Var, _rel: Var, _target: Var, _header: RecordHeader, removeSelfRelationships: Boolean = false): P = {
          val relationships =
            if (removeSelfRelationships) {
              producer.planFilter(_second,
                Not(Equals(_second.header.sourceNodeSlot(_rel).content.key, _second.header.targetNodeSlot(_rel).content.key)(CTNode))(CTNode),
                _second.header)
            } else _second
          val sourceSlot = _first.header.slotFor(_source)
          val sourceSlotInRel = _second.header.sourceNodeSlot(_rel)

          val sourceToRelHeader = _first.header ++ _second.header
          val sourceAndRel = producer.planJoin(_first, relationships, Seq((sourceSlot.content.key, sourceSlotInRel.content.key)), sourceToRelHeader)

          val targetSlot = _third.header.slotFor(_target)
          val targetSlotInRel = sourceAndRel.header.targetNodeSlot(_rel)

          producer.planJoin(sourceAndRel, _third, Seq((targetSlotInRel.content.key, targetSlot.content.key)), sourceToRelHeader ++ _third.header)
        }

        direction match {
          case Directed =>
            expandSource(first, second, third, source, rel, target, header)
          case Undirected =>
            val outgoing = expandSource(first, second, third, source, rel, target, header)
            val incoming = expandSource(third, second, first, target, rel, source, header, removeSelfRelationships = true)
            producer.planUnion(outgoing, incoming)
        }

      case op@flat.ExpandInto(source, rel, target, direction, sourceOp, header, relHeader) =>
        val in = process(sourceOp)
        val relationships = producer.planRelationshipScan(in, op.sourceGraph, rel, relHeader)

        val startNode = StartNode(rel)()
        val endNode = EndNode(rel)()

        direction match {
          case Directed =>
            producer.planJoin(in, relationships, Seq(source -> startNode, target -> endNode), header)

          case Undirected =>
            val outgoing = producer.planJoin(in, relationships, Seq(source -> startNode, target -> endNode), header)
            val incoming = producer.planJoin(in, relationships, Seq(target -> startNode, source -> endNode), header)
            producer.planUnion(outgoing, incoming)
        }

      case flat.InitVarExpand(source, edgeList, endNode, in, header) =>
        producer.planInitVarExpand(process(in), source, edgeList, endNode, header)

      case flat.BoundedVarExpand(
      source, rel, target, direction,
      lower, upper,
      sourceOp, relOp, targetOp,
      header, isExpandInto) =>
        val first = process(sourceOp)
        val second = process(relOp)
        val third = process(targetOp)

        val expandHeader = RecordHeader.from(
          header.selfWithChildren(source).map(_.content) ++
          header.selfWithChildren(rel).map(_.content) ++
          header.selfWithChildren(target).map(_.content): _*)
        val expand = producer.planExpandSource(first, second, third, source, rel, target, expandHeader)

        // remove each new starting node from data as it is the same as the previous end node, the storing schema will be
        // startNode, edge, endNode, edge_1, endNode_1, edge_2, ..., endNode_n
        val relAndTargetSlots = (expand.header.selfWithChildren(rel) ++ expand.header.selfWithChildren(target)).map(_.content).toIndexedSeq
        val relAndTarget = producer.planSelectFields(
          expand,
          relAndTargetSlots.map(slot => Var(slot.key.withoutType)(slot.cypherType)),
          RecordHeader.from(relAndTargetSlots: _*)
        )

        def iterate(i: Int, joinedExpands: P, oldTargetVar: Var, edgeVars: Set[Var]): P = {
          if (i == upper) return joinedExpands

          val newRelVar = Var(rel.name + "_" + i)(rel.cypherType)
          val newTargetVar = Var(target.name + "_" + i)(target.cypherType)

          val renamedRelVars = header.selfWithChildren(rel).map(_.withOwner(newRelVar))
          val renamedTargetVars = header.selfWithChildren(target).map(_.withOwner(newTargetVar))

          val renamedVars = (renamedRelVars ++ renamedTargetVars)
          val newHeader = RecordHeader.from(renamedVars.map(s => s.content): _*)

          val renamedData = producer.planBulkAlias(relAndTarget, relAndTarget.header.slots.map(_.content.key), renamedVars, newHeader)

          val joinedData = producer.planJoin(
            joinedExpands,
            renamedData,
            Seq((joinedExpands.header.slotFor(oldTargetVar).content.key, renamedData.header.sourceNodeSlot(newRelVar).content.key)),
            joinedExpands.header ++ newHeader
          )

          val withRelIsomorphism = producer.planFilter(
            joinedData,
            Ands(edgeVars.map { edge =>
              Not(Equals(joinedExpands.header.slotFor(edge).content.key, renamedData.header.slotFor(newRelVar).content.key)(CTNode))(CTNode): Expr
            }),
            joinedData.header)

          val withEmptyFields = producer.planSelect(
            joinedExpands,
            joinedExpands.header.slots.map(slot => slot.content.key) ++
              renamedVars.map(slot => As(NullLit()(slot.content.cypherType), slot.content.key)(CTWildcard)),
            joinedData.header)

          val unionedData = producer.planUnion(withEmptyFields, withRelIsomorphism)

          iterate(i+1, unionedData, newTargetVar, edgeVars + newRelVar)
        }

        iterate(1, expand, target, Set(rel))

      case flat.Optional(lhs, rhs, header) =>
        producer.planOptional(process(lhs), process(rhs), header)

      case flat.ExistsSubQuery(predicateField, lhs, rhs, header) =>
        producer.planExistsSubQuery(process(lhs), process(rhs), predicateField, header)

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in, header) =>
        producer.planOrderBy(process(in), sortItems, header)

      case flat.Skip(expr, in, header) =>
        producer.planSkip(process(in), expr, header)

      case flat.Limit(expr, in, header) =>
        producer.planLimit(process(in), expr, header)

      case x =>
        throw NotImplementedException(s"Physical planning of operator $x")
    }
  }

  private def relTypes(r: Var): Set[String] = r.cypherType match {
    case t: CTRelationship => t.types
    case _ => Set.empty
  }
}
