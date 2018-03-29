package org.opencypher.flink.physical

import org.opencypher.flink._
import org.opencypher.flink.physical.operators.CAPFPhysicalOperator
import org.opencypher.okapi.api.graph.{PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.okapi.logical.impl.{ConstructedEntity, Direction, LogicalExternalGraph, LogicalGraph}
import org.opencypher.okapi.relational.api.physical.{PhysicalOperatorProducer, PhysicalPlannerContext}
import org.opencypher.okapi.relational.impl.table.{ProjectedExpr, ProjectedField, RecordHeader}

case class CAPFPhysicalPlannerContext(
  session: CAPFSession,
  resolver: QualifiedGraphName => PropertyGraph,
  inputRecords: CAPFRecords,
  parameters: CypherMap) extends PhysicalPlannerContext[CAPFRecords]


object CAPFPhysicalPlannerContext {
  def from(
    resolver: QualifiedGraphName => PropertyGraph,
    inputRecords: CAPFRecords,
    parameters: CypherMap)(implicit session: CAPFSession):PhysicalPlannerContext[CAPFRecords] = {
    CAPFPhysicalPlannerContext(session, resolver, inputRecords, parameters)
  }
}

final class CAPFPhysicalOperatorProducer(implicit capf: CAPFSession)
  extends PhysicalOperatorProducer[CAPFPhysicalOperator, CAPFRecords, CAPFGraph, CAPFRuntimeContext] {

  /**
    * Starts the query execution based on the given records and an external graph.
    *
    * @param in backend-specific records
    * @param g  external (URI) reference to the input graph (e.g. the session graph)
    * @return start operator
    */
  override def planStart(in: CAPFRecords, g: LogicalExternalGraph): CAPFPhysicalOperator =
    operators.Start(in, g)

  /**
    * Starts the query execution based on empty records and an external graph.
    *
    * @param graph external (URI) reference to the input graph (e.g. the session graph)
    * @return start from unit operator
    */
  override def planStartFromUnit(graph: LogicalExternalGraph): CAPFPhysicalOperator =
    operators.StartFromUnit(graph)

  /**
    * Sets the source graph for the next query operation.
    *
    * @param in previous operator
    * @param g  external (URI) reference to a graph on which the query is continued
    * @return set source graph operator
    */
  override def planSetSourceGraph(in: CAPFPhysicalOperator, g: LogicalExternalGraph): CAPFPhysicalOperator =
    operators.SetSourceGraph(in, g)

  /**
    * Scans the node set of the input graph and returns all nodes that match the given CTNode type.
    *
    * @param in      previous operator
    * @param inGraph graph to scan nodes from
    * @param v       node variable carrying the node type to scan for
    * @param header  resulting record header
    * @return node scan operator
    */
  override def planNodeScan(in: CAPFPhysicalOperator, inGraph: LogicalGraph, v: Var, header: RecordHeader): CAPFPhysicalOperator =
    operators.Scan(in, inGraph, v, header)

  /**
    * Scans the relationship set of the input graph and returns all relationships that match the given CTRelationship
    * type.
    *
    * @param in      previous operator
    * @param inGraph graph to scan relationships from
    * @param v       node variable carrying the relationship type to scan for
    * @param header  resulting record header
    * @return relationship scan operator
    */
  override def planRelationshipScan(in: CAPFPhysicalOperator, inGraph: LogicalGraph, v: Var, header: RecordHeader): CAPFPhysicalOperator =
    operators.Scan(in, inGraph, v, header)

  /**
    * Creates an empty record set thereby disregarding the input. The records are described by the given record header.
    *
    * @param in     previous operator
    * @param header record header describing the created records
    * @return empty records operator
    */
  override def planEmptyRecords(in: CAPFPhysicalOperator, header: RecordHeader): CAPFPhysicalOperator =
    operators.EmptyRecords(in, header)

  /**
    * Renames the column identified by the given expression to the specified alias.
    *
    * @param in     previous operator
    * @param expr   expression to be aliased
    * @param alias  alias
    * @param header resulting record header
    * @return empty records operator
    */
  override def planAlias(in: CAPFPhysicalOperator, expr: Expr, alias: Var, header: RecordHeader): CAPFPhysicalOperator =
    operators.Alias(in, expr, alias, header)

  /**
    * The operator takes a set of (field, expression) aliases and renames the columns identified by a field to the
    * corresponding expression.
    *
    * @param in      previous operator
    * @param aliases set of aliases
    * @param header  resulting record header
    * @return remove aliases operator
    */
  override def planRemoveAliases(in: CAPFPhysicalOperator, aliases: Set[(ProjectedField, ProjectedExpr)], header: RecordHeader): CAPFPhysicalOperator =
    operators.RemoveAliases(in, aliases, header)

  /**
    * Filters the incoming rows according to the specified expression.
    *
    * @param in     previous operator
    * @param expr   expression to be evaluated
    * @param header resulting record header
    * @return filter operator
    */
  override def planFilter(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader): CAPFPhysicalOperator =
    operators.Filter(in, expr, header)

  /**
    * Selects the specified fields from the given records.
    *
    * @param in     previous operator
    * @param fields fields to select from the records (i.e., as specified in the RETURN clause)
    * @param header resulting record header
    * @return select fields operator
    */
  override def planSelectFields(in: CAPFPhysicalOperator, fields: IndexedSeq[Var], header: RecordHeader): CAPFPhysicalOperator =
    operators.SelectFields(in, fields, header)

  /**
    * Selects the specified graph from the input operator.
    *
    * @param in     previous operator
    * @param graphs graphs to select from the previous operator (i.e., as specified in the RETURN clause)
    * @return select graphs operator
    */
  override def planSelectGraphs(in: CAPFPhysicalOperator, graphs: Set[String]): CAPFPhysicalOperator =
    operators.SelectGraphs(in, graphs)

  /**
    * Evaluates the given expression and projects it to a new column in the input records.
    *
    * @param in     previous operator
    * @param expr   expression to evaluate
    * @param header resulting record header
    * @return project operator
    */
  override def planProject(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader): CAPFPhysicalOperator =
    operators.Project(in, expr, header)

  /**
    * Stores the graph identified by the given URI by the given name.
    *
    * @param in            previous operator
    * @param name          name to project graph to
    * @param qualifiedName reference to a graph (e.g. an external graph)
    * @return project external graph operator
    */
  override def planProjectExternalGraph(in: CAPFPhysicalOperator, name: String, qualifiedName: QualifiedGraphName): CAPFPhysicalOperator =
    operators.ProjectExternalGraph(in, name, qualifiedName)

  /**
    * Creates a new record containing the specified entities (i.e. as defined in a construction pattern).
    *
    * @param in       previous operator
    * @param toCreate entities to create
    * @param name     name of the resulting graph
    * @param schema   schema of the resulting graph
    * @param header   resulting record header
    * @return project pattern graph operator
    */
  override def planProjectPatternGraph(in: CAPFPhysicalOperator, toCreate: Set[ConstructedEntity], name: String, schema: Schema, header: RecordHeader): CAPFPhysicalOperator = ???

  /**
    * Groups the underlying records by the specified expressions and evaluates the given aggregate functions.
    *
    * @param in           previous operator
    * @param group        vars to group records by
    * @param aggregations aggregate functions
    * @param header       resulting record header
    * @return aggregate operator
    */
  override def planAggregate(in: CAPFPhysicalOperator, group: Set[Var], aggregations: Set[(Var, Aggregator)], header: RecordHeader): CAPFPhysicalOperator =
    operators.Aggregate(in, aggregations, group, header)

  /**
    * Performs a distinct operation on the specified fields.
    *
    * @param in     previous operator
    * @param fields fields to compute distinct on
    * @return distinct operator
    */
  override def planDistinct(in: CAPFPhysicalOperator, fields: Set[Var]): CAPFPhysicalOperator =
    operators.Distinct(in, fields)

  /**
    * Orders the underlying records by the given expressions.
    *
    * @param in        previous operator
    * @param sortItems fields to order records by
    * @param header    resulting record header
    * @return order by operator
    */
  override def planOrderBy(in: CAPFPhysicalOperator, sortItems: Seq[SortItem[Expr]], header: RecordHeader): CAPFPhysicalOperator =
    operators.OrderBy(in, sortItems)

  /**
    * Unwinds the given list of items into the specified var for each row in the input records.
    *
    * @param in     previous operator
    * @param list   list of items to unwind
    * @param item   var to project item to
    * @param header resulting record header
    * @return unwind operator
    */
  override def planUnwind(in: CAPFPhysicalOperator, list: Expr, item: Var, header: RecordHeader): CAPFPhysicalOperator =
    operators.Unwind(in, list, item, header)

  /**
    * Initializes the underlying records for a variable expand computation (e.g., (a)-[:A*1..3]->(b)).
    *
    * @param in       previous operator
    * @param source   variable to expand from (e.g. (a))
    * @param edgeList variable to identify column which later stores relationship identifiers of the computed paths
    * @param target   variable to expand into (e.g. (b))
    * @param header   resulting record header
    * @return init var expand operator
    */
  override def planInitVarExpand(in: CAPFPhysicalOperator, source: Var, edgeList: Var, target: Var, header: RecordHeader): CAPFPhysicalOperator =
    operators.InitVarExpand(in, source, edgeList, target, header)

  /**
    * Skips the given amount of rows in the input records. The number of rows is specified by an expression which can be
    * a literal or a query parameter.
    *
    * @param in     previous operator
    * @param expr   expression which contains or refers to the number of rows to skip
    * @param header resulting record header
    * @return skip operator
    */
  override def planSkip(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader): CAPFPhysicalOperator = ???

  /**
    * Limits the number of input records to the specified amount. The number of rows is specified by an expression which
    * can be a literal or a query parameter.
    *
    * @param in     previous operator
    * @param expr   expression which contains or refers to the maximum number of rows to return
    * @param header resulting record header
    * @return limit operator
    */
  override def planLimit(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader): CAPFPhysicalOperator = ???

  /**
    * Computes a cartesian product between the two input records.
    *
    * @param lhs    first previous operator
    * @param rhs    second previous operator
    * @param header resulting record header
    * @return cross operator
    */
  override def planCartesianProduct(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator, header: RecordHeader): CAPFPhysicalOperator = ???

  /**
    * Joins the two input records on node attribute values.
    *
    * @param lhs        first previous operator
    * @param rhs        second previous operator
    * @param predicates join predicates
    * @param header     resulting record header
    * @return value join operator
    */
  override def planValueJoin(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator, predicates: Set[expr.Equals], header: RecordHeader): CAPFPhysicalOperator =
    operators.ValueJoin(lhs, rhs, predicates, header)

  /**
    * Unions the input records.
    *
    * @param lhs first previous operator
    * @param rhs second previous operator
    * @return union operator
    */
  override def planUnion(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator): CAPFPhysicalOperator =
    operators.Union(lhs, rhs)

  /**
    * Joins the two input records on two columns, where `source` is solved in the first operator and `target` is solved
    * in the second operator.
    *
    * @param lhs    first previous operator
    * @param rhs    second previous operator
    * @param source variable solved by the first operator
    * @param rel    relationship variable
    * @param target variable solved by the second operator
    * @param header resulting record header
    * @return expand into operator
    */
  override def planExpandInto(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator, source: Var, rel: Var, target: Var, header: RecordHeader): CAPFPhysicalOperator = ???

  /**
    * Computes the result of an OPTIONAL MATCH where the first input is the non-optional part and the second input the
    * optional one.
    *
    * @param lhs    first previous operator
    * @param rhs    second previous operator
    * @param header resulting record header
    * @return optional operator
    */
  override def planOptional(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator, header: RecordHeader): CAPFPhysicalOperator = ???

  /**
    * Filters the rows of the first input by checking if there exists a corresponding row in the second input.
    *
    * @param lhs         first previous operator
    * @param rhs         second previous operator
    * @param targetField field that stores the (boolean) result of the evaluation
    * @param header      resulting record header
    * @return exists subquery operator
    */
  override def planExistsSubQuery(lhs: CAPFPhysicalOperator, rhs: CAPFPhysicalOperator, targetField: Var, header: RecordHeader): CAPFPhysicalOperator = ???

  /**
    * Expands the records in the first input (nodes) via the records in the second input (relationships) into the
    * records in the third input (nodes).
    *
    * @param first                   first previous operator
    * @param second                  second previous operator
    * @param third                   third previous operator
    * @param source                  node variable in the first input
    * @param rel                     relationship variable in the second input
    * @param target                  node variable in the third input
    * @param header                  resulting record header
    * @param removeSelfRelationships set true, iff loops shall be removed from the output
    * @return expand source operator
    */
  override def planExpandSource(first: CAPFPhysicalOperator, second: CAPFPhysicalOperator, third: CAPFPhysicalOperator, source: Var, rel: Var, target: Var, header: RecordHeader, removeSelfRelationships: Boolean): CAPFPhysicalOperator =
    operators.ExpandSource(first, second, third, source, rel, target, header, removeSelfRelationships)

  /**
    * Performs a bounded variable length path expression.
    *
    * @param first          first previous operator
    * @param second         second previous operator
    * @param third          third previous operator
    * @param rel            relationship variable to expand from
    * @param edgeList       refers to the column in which the path is stored
    * @param target         node variable in the third input
    * @param initialEndNode initial end node
    * @param lower          lower bound
    * @param upper          upper bound
    * @param direction      path direction
    * @param header         resulting record header
    * @param isExpandInto   true, iff the target variable is solved and can be replaced by a filter
    * @return bounded var expand operator
    */
  override def planBoundedVarExpand(first: CAPFPhysicalOperator, second: CAPFPhysicalOperator, third: CAPFPhysicalOperator, rel: Var, edgeList: Var, target: Var, initialEndNode: Var, lower: Int, upper: Int, direction: Direction, header: RecordHeader, isExpandInto: Boolean): CAPFPhysicalOperator =
    operators.BoundedVarExpand(first, second, third, rel, edgeList, target, initialEndNode, lower, upper, direction, header, isExpandInto)
}
