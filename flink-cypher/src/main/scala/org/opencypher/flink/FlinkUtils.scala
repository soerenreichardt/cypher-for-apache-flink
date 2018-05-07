package org.opencypher.flink

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.types.Row
import org.opencypher.flink.physical.CAPFRuntimeContext
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.ir.api.expr.{Expr, Param}
import org.opencypher.okapi.relational.impl.table.RecordHeader

object FlinkUtils {

  def fromFlinkType(flinkType: TypeInformation[_]): Option[CypherType] = {
    val cypherType = flinkType match {
      case Types.STRING => Some(CTString)
      case Types.INT => Some(CTInteger)
      case Types.LONG => Some(CTInteger)
      case Types.BOOLEAN => Some(CTBoolean)
      case Types.DOUBLE => Some(CTFloat)
      case PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO => Some(CTList(CTBoolean))
      case PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO => Some(CTList(CTFloat))
      case PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO => Some(CTList(CTFloat))
      case PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO => Some(CTList(CTInteger))
      case PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO => Some(CTList(CTInteger))
      case basicArray: BasicArrayTypeInfo[_, _] => Some(CTList(fromFlinkType(basicArray.getComponentInfo).get))
      case objArray: ObjectArrayTypeInfo[_, _] => Some(CTList(fromFlinkType(objArray.getComponentInfo).get))

//      TODO: other datatypes
      case _ => None
    }

    cypherType
  }

  def toFlinkType(ct: CypherType): TypeInformation[_] = ct match {
    case CTNull | CTVoid => throw NotImplementedException("")
    case _ => ct.material match {
      case CTString => Types.STRING
      case CTInteger => Types.LONG
      case CTBoolean => Types.BOOLEAN
      case CTFloat => Types.DOUBLE
      case _: CTNode => Types.LONG
      case _: CTRelationship => Types.LONG
      case l: CTList =>
        l.elementType match {
          case CTString =>
            Types.OBJECT_ARRAY(Types.STRING)
          case cType =>
            Types.PRIMITIVE_ARRAY(toFlinkType(cType))
        }


      case x =>
        throw NotImplementedException("")
    }
  }

  implicit class CypherRow(r: Row) {
    def getCypherValue(expr: Expr, header: RecordHeader)(implicit context: CAPFRuntimeContext): CypherValue = {
      expr match {
        case Param(name) => context.parameters(name)
        case _ =>
          header.slotsFor(expr).headOption match {
            case None => throw IllegalArgumentException(s"slot for $expr")
            case Some(slot) =>
              val index = slot.index
              CypherValue(r.getField(index))
          }
      }
    }
  }

  def cypherTypeForColumn(table: Table, columnName: String): CypherType = {
    val cypherType = cypherCompatibleDataType(table.getSchema.getType(columnName).getOrElse(throw IllegalArgumentException("")))
        .flatMap(fromFlinkType(_))
    cypherType.getOrElse(
      throw IllegalArgumentException("")
    )
  }

  def cypherCompatibleDataType(flinkType: TypeInformation[_]): Option[TypeInformation[_]] = flinkType match {
    case Types.BYTE | Types.SHORT | Types.INT | Types.DECIMAL => Some(Types.LONG)
    case Types.FLOAT => Some(Types.DOUBLE)
    case compatible if fromFlinkType(flinkType).isDefined => Some(compatible)
  }

}
