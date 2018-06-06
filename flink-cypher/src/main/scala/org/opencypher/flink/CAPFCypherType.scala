package org.opencypher.flink

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.table.api.Types
import org.opencypher.flink.FlinkUtils.fromFlinkType
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.NotImplementedException

object CAPFCypherType {
  implicit class RichCypherType(val ct: CypherType) extends AnyVal {
    def toFlinkType: Option[TypeInformation[_]] = ct match {
      case CTNull | CTVoid => Some(Types.BOOLEAN) // TODO: boolean is just a dummy
      case _ => ct.material match {
        case CTString => Some(Types.STRING)
        case CTInteger => Some(Types.LONG)
        case CTBoolean => Some(Types.BOOLEAN)
        case CTFloat => Some(Types.DOUBLE)
        case _: CTNode => Some(Types.LONG)
        case _: CTRelationship => Some(Types.LONG)
        case l: CTList =>
          l.elementType match {
            case CTString =>
              Some(Types.OBJECT_ARRAY(Types.STRING))
            case cType =>
              Some(Types.PRIMITIVE_ARRAY(cType.getFlinkType))
          }

        case _ =>
          None
      }
    }

    def getFlinkType: TypeInformation[_] = toFlinkType match {
      case Some(t) => t
      case None => throw NotImplementedException(s"Mapping of CypherType $ct to Spark type")
    }

    def isFlinkCompatible: Boolean = toFlinkType.isDefined
  }

  val supportedTypes = Seq(
    Types.BYTE,
    Types.SHORT,
    Types.INT,
    Types.LONG,
    Types.FLOAT,
    Types.DOUBLE,
    Types.STRING,
    Types.BOOLEAN
  )

  implicit class RichDataType(val tpe: TypeInformation[_]) extends AnyVal {
    def toCypherType(): Option[CypherType] = {
      val result = tpe match {
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

      result
    }

    def cypherCompatibleDataType: Option[TypeInformation[_]] = tpe match {
      case Types.BYTE | Types.SHORT | Types.INT | Types.DECIMAL => Some(Types.LONG)
      case Types.FLOAT => Some(Types.DOUBLE)
      case compatible if tpe.toCypherType().isDefined => Some(compatible)
      case _ => None
    }
  }
}