package org.opencypher.flink.physical.operators

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{Expression, UnresolvedFieldReference}
import org.apache.flink.types.Row
import org.opencypher.flink.FlinkSQLExprMapper._
import org.opencypher.flink.FlinkUtils._
import org.opencypher.flink.TableOps._
import org.opencypher.flink.physical.{CAPFPhysicalResult, CAPFRuntimeContext}
import org.opencypher.flink.schema.EntityTable._
import org.opencypher.flink.{CAPFRecords, CAPFSession, ColumnName}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherInteger, CypherList}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException, SchemaException}
import org.opencypher.okapi.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.impl.table._

private[flink] abstract class UnaryPhysicalOperator extends CAPFPhysicalOperator {

  def in: CAPFPhysicalOperator

  override def execute(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = executeUnary(in.execute)

  def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult
}

final case class NodeScan(in: CAPFPhysicalOperator, v: Var, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val graph = prev.workingGraph
    val records = v.cypherType match {
      case n: CTNode => graph.nodes(v.name, n)
      case other => throw IllegalArgumentException("Node variable", other)
    }
    if (header != records.header) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed record header based on graph schema: ${header.slots.map(_.content).mkString("[", ",", "]")}
           |  - Actual record header: ${records.header.slots.map(_.content).mkString("[", ",", "]")}
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
    if (header != records.header) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed record header based on graph schema: ${header.slots.map(_.content).mkString("[", ",", "]")}
           |  - Actual record header: ${records.header.slots.map(_.content).mkString("[", ",", "]")}
        """.stripMargin)
    }
    CAPFPhysicalResult(records, graph, v.cypherType.graph.get)
  }
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

final case class Alias(in: CAPFPhysicalOperator, aliases: Seq[(Expr,  Var)], header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
   prev.mapRecordsWithDetails { records =>
     val inHeader = records.header

     val newData = aliases.foldLeft(records.data) {
       case (acc, (expr, alias)) =>
         val oldSlot = inHeader.slotsFor(expr).head
         val newSlot = header.slotsFor(alias).head

         val oldColumnName = ColumnName.of(oldSlot)
         val newColumnName = ColumnName.of(newSlot)

         if (records.data.columns.contains(oldColumnName)) {
           acc.safeRenameColumn(oldColumnName, newColumnName)
         } else {
           throw IllegalArgumentException(s"a column with name $oldColumnName")
         }
     }

     CAPFRecords.verifyAndCreate(header, newData)(records.capf)
   }
  }
}

final case class BulkAlias(in: CAPFPhysicalOperator, exprs: Seq[Expr], aliases: Seq[RecordSlot], header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val oldSlots = exprs.map(records.header.slotsFor(_).head)

      val newSlots = aliases

      val oldColumnNames = oldSlots.map(ColumnName.of)
      val newColumnNames = newSlots.map(ColumnName.of)

      val newData = if (oldColumnNames.forall(records.data.columns.contains)) {
        records.data.safeRenameColumns(oldColumnNames, newColumnNames)
      } else {
        throw IllegalArgumentException(s"some columns with name $oldColumnNames")
      }

      CAPFRecords.verifyAndCreate(header, newData)(records.capf)
    }
  }
}

final case class Select(in: CAPFPhysicalOperator, exprs: Seq[Expr], header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
//      val propertyFields = exprs.flatMap {
//        _ match {
//          case v: Var =>
//            header.slotFor(v).content match {
//              case _: OpaqueField => header.childSlots(v).map(slot => slot.content.key)
//              case _ => Seq()
//            }
//          case other => Seq()
//        }
//      }
//
//      val alignedWithHeader = exprs ++ propertyFields
//
//      val asFlinkExpression = alignedWithHeader.distinct.map(_.asFlinkSQLExpr(header, records.data, context))
//      val newData = records.data.select(asFlinkExpression: _*)

      val data = records.data

      val columns = exprs.map(_.asFlinkSQLExpr(header, data, context))
      val newData = data.select(columns: _*)

      CAPFRecords.verifyAndCreate(header, newData)(records.capf)
    }
  }
}

final case class Drop(in: CAPFPhysicalOperator, dropFields: Seq[Expr], header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val columnsToDrop = dropFields
        .map(ColumnName.of)
        .filter(records.data.columns.contains)

      val withDroppedFields = records.data.safeDropColumns(columnsToDrop: _*)
      CAPFRecords.verifyAndCreate(header, withDroppedFields)(records.capf)
    }
  }
}

final case class ReturnGraph(in: CAPFPhysicalOperator) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    CAPFPhysicalResult(CAPFRecords.empty(header)(prev.records.capf), prev.workingGraph, prev.workingGraphName)
  }

  override def header: RecordHeader = RecordHeader.empty
}

final case class EmptyRecords(in: CAPFPhysicalOperator, header: RecordHeader)(implicit capf: CAPFSession)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult =
    prev.mapRecordsWithDetails(_ => CAPFRecords.empty(header))

}

final case class RemoveAliases(
  in: CAPFPhysicalOperator,
  dependentFields: Set[(ProjectedField, ProjectedExpr)],
  header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val renamed = dependentFields.foldLeft(records.data) {
        case (table, (v, expr)) =>
          table.safeRenameColumn(ColumnName.of(v), ColumnName.of(expr))
      }

      CAPFRecords.verifyAndCreate(header, renamed)(records.capf)
    }
  }
}

final case class Filter(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {
  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val filteredData = records.data.filter(expr.asFlinkSQLExpr(header, records.data, context))

      CAPFRecords.verifyAndCreate(header, filteredData)(records.capf)
    }
  }
}

final case class Project(in: CAPFPhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val headerNames = header.slotsFor(expr).map(ColumnName.of)
      val dataNames = records.data.columns

      val newData = headerNames.diff(dataNames) match {
        case Seq(one) =>
          val newCol: Expression = expr.asFlinkSQLExpr(header, records.data, context) as Symbol(one)
          val columnsToSelect = records.data.columns.map(UnresolvedFieldReference(_)) :+ newCol // TODO: missmatch between expression and column/table

          records.data.select(columnsToSelect: _*)
        case seq if seq.isEmpty => throw IllegalStateException(s"Did not find a slot for expression $expr in $headerNames")
        case seq => throw IllegalStateException(s"Got multiple slots for expression $expr: $seq")
      }

      CAPFRecords.verifyAndCreate(header, newData)(records.capf)
    }
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
      val inData = records.data

      def withInnerExpr(expr: Expr)(f: Expression => Expression) =
        f(expr.asFlinkSQLExpr(records.header, inData, context))

      val columns =
        if (group.nonEmpty) {
          group.flatMap { expr =>
            val withChildren = records.header.selfWithChildren(expr).map(_.content)
            withChildren.map(e => UnresolvedFieldReference(ColumnName.of(e)))
          }
        } else null

      val data =
        if (columns != null) {
          Left(inData.groupBy(columns.toSeq: _*))
        } else Right(inData)

      val flinkAggFunctions = aggregations.map {
        case (to, inner) =>
          val columnName = Symbol(ColumnName.from(to.name))
          inner match {
            case Avg(expr) =>
              withInnerExpr(expr)(
                _.avg
                  .cast(toFlinkType(to.cypherType))
                  .as(columnName)
              )

            case CountStar(_) =>
              withInnerExpr(IntegerLit(0)(CTInteger))(_.count.as(columnName))

            case Count(expr, distinct) => withInnerExpr(expr)( column =>
              column.count
                .as(columnName)
            )

            case Max(expr) =>
              withInnerExpr(expr)(_.max.as(columnName))

            case Min(expr) =>
              withInnerExpr(expr)(_.min.as(columnName))

            case Sum(expr) =>
              withInnerExpr(expr)(_.sum.as(columnName))

            case Collect(expr, distinct) => withInnerExpr(expr) { column =>
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

      CAPFRecords.verifyAndCreate(header, aggregated)(records.capf)
    }
  }
}

final case class Distinct(in: CAPFPhysicalOperator, fields: Set[Var])
  extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      CAPFRecords.verifyAndCreate(prev.records.header, records.data.distinct())(records.capf)
    }
  }

}

final case class OrderBy(in: CAPFPhysicalOperator, sortItems: Seq[SortItem[Expr]])
  extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    val getColumnName = (expr: Var) => ColumnName.of(prev.records.header.slotFor(expr))

    prev.mapRecordsWithDetails { records =>

      val sortExpression = sortItems.map {
        case Asc(expr: Var) => expr.asFlinkSQLExpr(records.header, records.data, context).asc
        case Desc(expr: Var) => expr.asFlinkSQLExpr(records.header, records.data, context).desc
        case other => throw IllegalArgumentException("ASC or DESC", other)
      }

      val sortedData = records.toTable().orderBy(sortExpression: _*)
      CAPFRecords.verifyAndCreate(header, sortedData)(records.capf)
    }
  }
}

final case class Unwind(in: CAPFPhysicalOperator, list: Expr, item: Var, header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val itemColumn = ColumnName.of(header.slotFor(item))
      val newData = list match {
        case Param(name) =>
          context.parameters(name).as[CypherList] match {
            case Some(l) =>
              val flinkType = toFlinkType(item.cypherType)
              val nullable = item.cypherType.isNullable

              implicit val typeInfo = new RowTypeInfo(flinkType)
              val rowList = l.unwrap.map(v => Row.of(v.asInstanceOf[AnyRef]))
              val rowDataSet = records.capf.env.fromCollection(rowList)
              val tableSchema = UnresolvedFieldReference(itemColumn)
              val table = records.capf.tableEnv.fromDataSet(rowDataSet, tableSchema)
              val tableWithCorrectType = table.select(tableSchema)
              records.data.cross(tableWithCorrectType)(records.capf)

            case None =>
              throw IllegalArgumentException("a list", list)
          }

        case expr =>
          val listColumn = expr.asFlinkSQLExpr(records.header, records.data, context)

          records.data.safeAddColumn(itemColumn, listColumn.flatten())
      }

      CAPFRecords.verifyAndCreate(header, newData)(records.capf)
    }
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
      CAPFRecords.verifyAndCreate(header, records.data.offset(skip.toInt))(records.capf)
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
      CAPFRecords.verifyAndCreate(header, records.data.fetch(limit.toInt))(records.capf)
    }
  }
}

final case class FromGraph(in: CAPFPhysicalOperator, graph: LogicalCatalogGraph) extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPFPhysicalResult)(implicit context: CAPFRuntimeContext): CAPFPhysicalResult =
    CAPFPhysicalResult(prev.records, resolve(graph.qualifiedGraphName), graph.qualifiedGraphName)
}

//object IdGenerator {
//
//  var lastIds = new java.util.concurrent.ConcurrentHashMap[Int, Long]
//  var parallelism: Int = 0
//  var baseValue: Long = 0
//
//  def increment(parId: Int, parallelism: Int): Long = {
//    if (this.parallelism == 0) this.parallelism = parallelism
//
//    if (this.parallelism == parallelism) {
//      val mapValue = lastIds.get(parId)
//      if (mapValue == null) {
//        lastIds.put(parId, baseValue + parId.toLong)
//      } else {
//        lastIds.put(parId, baseValue + parId * (((lastIds.get(parId) - baseValue) / parallelism) + 1))
//      }
//    } else {
//      baseValue = ???
//    }
//
//    lastIds.get(parId)
//  }
//}