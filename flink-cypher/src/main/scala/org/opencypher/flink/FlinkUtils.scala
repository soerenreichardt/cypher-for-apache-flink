package org.opencypher.flink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{Table, Types}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}

object FlinkUtils {

  def fromFlinkType(flinkType: TypeInformation[_]): Option[CypherType] = {
    val cypherType = flinkType match {
      case Types.STRING => Some(CTString)
      case Types.INT => Some(CTInteger)
      case Types.LONG => Some(CTInteger)
      case Types.BOOLEAN => Some(CTBoolean)
      case Types.DOUBLE => Some(CTFloat)
//      TODO: other datatypes
      case _ => None
    }

    cypherType
  }

  def toFlinkType(ct: CypherType): TypeInformation[_] = ct match {
    case CTNull | CTVoid => throw NotImplementedException("")
    case _ => ct.material match {
      case CTString => Types.STRING
      case CTInteger => Types.INT
      case CTBoolean => Types.BOOLEAN
      case CTFloat => Types.DOUBLE
      case _: CTNode => Types.LONG
      case _: CTRelationship => Types.LONG

      case x =>
        throw NotImplementedException("")
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
