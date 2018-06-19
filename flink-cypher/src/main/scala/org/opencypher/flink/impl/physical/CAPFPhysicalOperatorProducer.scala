package org.opencypher.flink.impl.physical

import org.opencypher.flink._
import org.opencypher.flink.api.io.FlinkCypherTable.FlinkTable
import org.opencypher.flink.impl.{CAPFGraph, CAPFRecords, CAPFSession}
import org.opencypher.flink.impl.physical.operators.CAPFPhysicalOperator
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr.{Aggregator, AliasExpr, Expr, Var}
import org.opencypher.okapi.ir.impl.QueryCatalog
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.physical.{PhysicalOperatorProducer, PhysicalPlannerContext}
import org.opencypher.okapi.relational.impl.physical._
import org.opencypher.okapi.relational.impl.table.RecordHeader

case class CAPFPhysicalPlannerContext(
  session: CAPFSession,
  catalog: QueryCatalog,
  inputRecords: CAPFRecords,
  parameters: CypherMap,
  constructedGraphPlans: collection.mutable.Map[QualifiedGraphName, CAPFPhysicalOperator]) extends PhysicalPlannerContext[CAPFPhysicalOperator, CAPFRecords]


object CAPFPhysicalPlannerContext {
  def from(
    catalog: QueryCatalog,
    inputRecords: CAPFRecords,
    parameters: CypherMap)(implicit session: CAPFSession):PhysicalPlannerContext[CAPFPhysicalOperator, CAPFRecords] = {
    CAPFPhysicalPlannerContext(session, catalog, inputRecords, parameters, collection.mutable.Map.empty)
  }
}

final class CAPFPhysicalOperatorProducer(implicit capf: CAPFSession)
  extends PhysicalOperatorProducer[FlinkTable, CAPFPhysicalOperator, CAPFRecords, CAPFGraph, CAPFRuntimeContext] {


  override def planCartesianProduct(
    lhs: CAPFPhysicalOperator,
    rhs: CAPFPhysicalOperator,
    header: RecordHeader): CAPFPhysicalOperator = operators.CartesianProduct(lhs, rhs, header)

  override def planDrop(
    in: CAPFPhysicalOperator,
    dropFields: Set[Expr],
    header: RecordHeader
  ): CAPFPhysicalOperator = operators.Drop(in, dropFields, header)

  override def planRenameColumns(
    in: CAPFPhysicalOperator,
    renameExprs: Map[Expr, String],
    header: RecordHeader
  ): CAPFPhysicalOperator = operators.RenameColumns(in, renameExprs, header)

  override def planSelect(in: CAPFPhysicalOperator, exprs: List[Expr], header: RecordHeader): CAPFPhysicalOperator =
    operators.Select(in, exprs, header)

  override def planReturnGraph(in: CAPFPhysicalOperator): CAPFPhysicalOperator =
    operators.ReturnGraph(in)

  override def planEmptyRecords(in: CAPFPhysicalOperator, header: RecordHeader): CAPFPhysicalOperator =
    operators.EmptyRecords(in, header)

  override def planStart(
    qgnOpt: Option[QualifiedGraphName] = None,
    in: Option[CAPFRecords] = None,
    header: RecordHeader): CAPFPhysicalOperator =
    operators.Start(qgnOpt.getOrElse(capf.emptyGraphQgn), in)

  // TODO: Make catalog usage consistent between Start/FROM GRAPH
  override def planFromGraph(in: CAPFPhysicalOperator, g: LogicalCatalogGraph): CAPFPhysicalOperator =
    operators.FromGraph(in, g)

  override def planNodeScan(
    in: CAPFPhysicalOperator,
    inGraph: LogicalGraph,
    v: Var,
    header: RecordHeader): CAPFPhysicalOperator = operators.NodeScan(in, v, header)

  override def planRelationshipScan(
    in: CAPFPhysicalOperator,
    inGraph: LogicalGraph,
    v: Var,
    header: RecordHeader): CAPFPhysicalOperator = operators.RelationshipScan(in, v, header)

  override def planAliases(in: CAPFPhysicalOperator, aliases: Seq[AliasExpr], header: RecordHeader): CAPFPhysicalOperator =
    operators.Alias(in, aliases, header)

  override def planAddColumn(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader): CAPFPhysicalOperator =
    operators.AddColumn(in, expr, header)

  override def planCopyColumn(in: CAPFPhysicalOperator, from: Expr, to: Expr, header: RecordHeader): CAPFPhysicalOperator =
    operators.CopyColumn(in, from, to, header)

  override def planConstructGraph(
    in: CAPFPhysicalOperator,
    onGraph: CAPFPhysicalOperator,
    construct: LogicalPatternGraph): CAPFPhysicalOperator = {
    operators.ConstructGraph(in, onGraph, construct)
  }

  override def planAggregate(in: CAPFPhysicalOperator, group: Set[Var], aggregations: Set[(Var, Aggregator)], header: RecordHeader): CAPFPhysicalOperator = operators.Aggregate(in, aggregations, group, header)

  override def planFilter(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader): CAPFPhysicalOperator =
    operators.Filter(in, expr, header)

  override def planJoin(
    lhs: CAPFPhysicalOperator,
    rhs: CAPFPhysicalOperator,
    joinColumns: Seq[(Expr, Expr)],
    header: RecordHeader,
    joinType: JoinType): CAPFPhysicalOperator = {

    operators.Join(lhs, rhs, joinColumns, header, joinType)
  }

  override def planDistinct(in: CAPFPhysicalOperator, fields: Set[Var]): CAPFPhysicalOperator =
    operators.Distinct(in, fields)

  override def planTabularUnionAll(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator): CAPFPhysicalOperator =
    operators.TabularUnionAll(lhs, rhs)

  override def planExistsSubQuery(
    lhs: CAPFPhysicalOperator,
    rhs: CAPFPhysicalOperator,
    targetField: Var,
    header: RecordHeader): CAPFPhysicalOperator = operators.ExistsSubQuery(lhs, rhs, targetField, header)

  override def planOrderBy(
    in: CAPFPhysicalOperator,
    sortItems: Seq[SortItem[Expr]],
    header: RecordHeader): CAPFPhysicalOperator = operators.OrderBy(in, sortItems)

  override def planSkip(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader): CAPFPhysicalOperator =
    operators.Skip(in, expr, header)

  override def planLimit(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader): CAPFPhysicalOperator =
    operators.Limit(in, expr, header)

  /**
    * Performs a UNION ALL over graphs.
    *
    * @param graphs graphs to perform UNION ALL over together
    * @param qgn    name for the union graph
    * @return union all operator
    */
  override def planGraphUnionAll(graphs: List[CAPFPhysicalOperator], qgn: QualifiedGraphName): CAPFPhysicalOperator = ???
}
