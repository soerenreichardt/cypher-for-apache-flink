package org.opencypher.flink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
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
}