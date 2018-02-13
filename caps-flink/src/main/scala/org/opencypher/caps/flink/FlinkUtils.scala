package org.opencypher.caps.flink

import org.apache.flink.api.common.typeinfo.{TypeInformation}
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.Table
import org.opencypher.caps.api.types._

object FlinkUtils {

  def fromFlinkType(flinkType: TypeInformation[_]): Option[CypherType] = {
    val cypherType = flinkType match {
      case Types.STRING => Some(CTString)
      case Types.LONG => Some(CTInteger)
      case Types.BOOLEAN => Some(CTBoolean)
      case Types.DOUBLE => Some(CTFloat)
//      TODO: other datatypes
    }

    cypherType
  }

  def cypherTypeForColumn(table: Table, columnName: String): CypherType = {
    val cypherType = cypherCompatibleDataType(table.getSchema.getType(columnName).getOrElse(throw IllegalArgumentException))
        .flatMap(fromFlinkType(_))
    cypherType.getOrElse(
      throw IllegalArgumentException
    )
  }

  def cypherCompatibleDataType(flinkType: TypeInformation[_]): Option[TypeInformation[_]] = flinkType match {
    case Types.BYTE | Types.SHORT | Types.INT | Types.DECIMAL => Some(Types.LONG)
    case Types.FLOAT => Some(Types.DOUBLE)
    case compatible if fromFlinkType(flinkType).isDefined => Some(compatible)
  }

}
