package org.opencypher.flink.impl.physical.operators

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{Expression, ResolvedFieldReference, UnresolvedFieldReference}
import org.apache.flink.types.Row
import org.opencypher.flink.impl.FlinkSQLExprMapper._
import org.opencypher.flink.impl.convert.FlinkConversions._
import org.opencypher.flink.impl.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.flink.impl.{CAPFRecords, CAPFSession}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherInteger, CypherList}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException, SchemaException}
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.impl.table._

private[flink] abstract class UnaryPhysicalOperator extends CAPFPhysicalOperator {

  def in: CAPFPhysicalOperator

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = executeUnary(in.execute)

  def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult
}

final case class Cache(in: CAPFPhysicalOperator) extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    context.cache.getOrElse(in, {
      prev.records.cache()
      context.cache(in) = prev
      prev
    })
  }
}

final case class NodeScan(in: CAPFPhysicalOperator, v: Var, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val graph = prev.workingGraph
    val records = v.cypherType match {
      case n: CTNode => graph.nodes(v.name, n)
      case other => throw IllegalArgumentException("Node variable", other)
    }
    if (header.!=(records.header)) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed record header based on graph schema: ${header.pretty}
           |  - Actual record header: ${records.header.pretty}
         """.stripMargin)
    }
    CAPFPhysicalResult(records, graph, prev.workingGraphName)
  }
}

final case class RelationshipScan(in: CAPFPhysicalOperator, v: Var, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val graph = prev.workingGraph
    val records = v.cypherType match {
      case r: CTRelationship => graph.relationships(v.name, r)
      case other => throw IllegalArgumentException("Relationship variable", other)
    }
    if (header.!=(records.header)) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed record header based on graph schema: ${header.pretty}
           |  - Actual record header: ${records.header.pretty}
        """.stripMargin)
    }
    CAPFPhysicalResult(records, graph, v.cypherType.graph.get)
  }
}

final case class Alias(in: CAPFPhysicalOperator, aliases: Seq[AliasExpr], header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
   prev.mapRecordsWithDetails { records => CAPFRecords(header, records.table)(records.capf) }
  }
}

final case class Unwind(in: CAPFPhysicalOperator, list: Expr, item: Var, header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val unwindedColumn = list match {
        case p@Param(name) if p.cypherType.subTypeOf(CTList(CTAny)).maybeTrue =>
          context.parameters(name) match {
            case CypherList(l) =>
              val session = records.capf
              implicit val typeInfo: RowTypeInfo = new RowTypeInfo(item.cypherType.getFlinkType)
              val listDS = session.env.fromCollection(l.unwrap.map(v => Row.of(v.asInstanceOf[AnyRef])))
              session.tableEnv.fromDataSet(listDS, Symbol(header.column(item)))
          }
        case notAList => throw IllegalArgumentException("a Cypher list", notAList)
      }
      implicit val session: CAPFSession = records.capf
      CAPFRecords(header, unwindedColumn)
    }
  }
}

final case class AddColumn(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records => records.addColumn(expr)(context.parameters) }
  }

}

final case class CopyColumn(in: CAPFPhysicalOperator, from: Expr, to: Expr, header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records => records.copyColumn(from, to)(context.parameters) }
  }
}

final case class Drop(in: CAPFPhysicalOperator, dropFields: Set[Expr], header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records => records.drop(dropFields.toSeq: _*) }
  }
}

final case class RenameColumns(in: CAPFPhysicalOperator, renameExprs: Map[Expr, String], header: RecordHeader) extends UnaryPhysicalOperator {
  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records => records.renameColumns(renameExprs.toSeq: _*)(Some(header)) }
  }
}

final case class Filter(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {
  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records => records.filter(expr)(context.parameters) }
  }
}

final case class ReturnGraph(in: CAPFPhysicalOperator) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    CAPFPhysicalResult(CAPFRecords.empty(header)(prev.records.capf), prev.workingGraph, prev.workingGraphName)
  }

  override def header: RecordHeader = RecordHeader.empty
}

final case class Select(in: CAPFPhysicalOperator, exprs: List[Expr], header: RecordHeader) extends UnaryPhysicalOperator {
  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records => records.select(exprs.head, exprs.tail: _*) }
  }
}

final case class Distinct(in: CAPFPhysicalOperator, fields: Set[Var])
  extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records => records.distinct }
  }
}



final case class Aggregate(
  in: CAPFPhysicalOperator,
  aggregations: Set[(Var, Aggregator)],
  group: Set[Var],
  header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val inData = records.table

      def withInnerExpr(expr: Expr)(f: Expression => Expression) =
        f(expr.asFlinkSQLExpr(records.header, inData, context.parameters))

      val columns =
        if (group.nonEmpty) {
          group.flatMap { expr =>
            val withChildren = records.header.ownedBy(expr)
            withChildren.map(e => UnresolvedFieldReference(header.column(e)))
          }
        } else null

      val data =
        if (columns != null) {
          Left(inData.groupBy(columns.toSeq: _*))
        } else Right(inData)

      val flinkAggFunctions = aggregations.map {
        case (to, inner) =>
          val columnName = Symbol(header.column(to))
          inner match {
            case Avg(expr) =>
              withInnerExpr(expr)(
                _.avg
                  .cast(to.cypherType.getFlinkType)
                  .as(columnName)
              )

            case CountStar(_) =>
              withInnerExpr(IntegerLit(0)(CTInteger))(_.count.as(columnName))

            case Count(expr, _) => withInnerExpr(expr)( column =>
              column.count
                .as(columnName)
            )

            case Max(expr) =>
              withInnerExpr(expr)(_.max.as(columnName))

            case Min(expr) =>
              withInnerExpr(expr)(_.min.as(columnName))

            case Sum(expr) =>
              withInnerExpr(expr)(_.sum.as(columnName))

            case Collect(expr, _) => withInnerExpr(expr) { column =>
              val list = array(column)
              list as columnName
            }
            case x =>
              throw NotImplementedException(s"Aggregation function $x")

          }
      }

      val aggregated = data.fold(
        _.select((columns ++ flinkAggFunctions).toSeq: _*),
        _.select(flinkAggFunctions.toSeq: _*)
      )

      CAPFRecords(header, aggregated)(records.capf)
    }
  }
}



final case class OrderBy(in: CAPFPhysicalOperator, sortItems: Seq[SortItem[Expr]])
  extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records => records.orderBy(sortItems: _*) }
  }
}

final case class Skip(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val skip: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(l) => l
          case other => throw IllegalArgumentException("a CypherInteger", other)
        }
      case other => throw IllegalArgumentException("an integer literal or parameter", other)
    }

    prev.mapRecordsWithDetails { records =>
      CAPFRecords(header, records.table.offset(skip.toInt))(records.capf)
    }
  }
}

final case class Limit(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val limit: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(v) => v
          case other => throw IllegalArgumentException("a CypherInteger", other)
        }
      case other => throw IllegalArgumentException("an integer literal or parameter", other)
    }


    prev.mapRecordsWithDetails { records =>
      CAPFRecords(header, records.table.fetch(limit.toInt))(records.capf)
    }
  }
}

final case class EmptyRecords(in: CAPFPhysicalOperator, header: RecordHeader)(implicit capf: CAPFSession)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult =
    prev.mapRecordsWithDetails(_ => CAPFRecords.empty(header))
}

final case class FromGraph(in: CAPFPhysicalOperator, graph: LogicalCatalogGraph) extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult =
    CAPFPhysicalResult(prev.records, resolve(graph.qualifiedGraphName), graph.qualifiedGraphName)
}