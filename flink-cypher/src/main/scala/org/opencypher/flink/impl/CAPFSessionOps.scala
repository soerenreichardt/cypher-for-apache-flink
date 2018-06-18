package org.opencypher.flink.impl

import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr.{Expr, Var}

trait CAPFSessionOps {


  def filter(
    graph: PropertyGraph,
    in: CypherRecords,
    expr: Expr,
    queryParameters: CypherMap): CypherRecords

  def select(
    graph: PropertyGraph,
    in: CypherRecords,
    fields: List[Var],
    queryParameters: CypherMap): CypherRecords

  def project(
    graph: PropertyGraph,
    in: CypherRecords,
    expr: Expr,
    queryParameters: CypherMap): CypherRecords

  def alias(
    graph: PropertyGraph,
    in: CypherRecords,
    alias: (Expr, Var),
    queryParameters: CypherMap): CypherRecords

}
