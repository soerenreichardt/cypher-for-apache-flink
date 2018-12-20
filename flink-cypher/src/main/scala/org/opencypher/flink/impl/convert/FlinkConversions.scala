/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.flink.impl.convert

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.expressions.ResolvedFieldReference
import org.apache.orc.TypeDescription
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader

object FlinkConversions {
  implicit class CypherTypeOps(val ct: CypherType) extends AnyVal {
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

  implicit class TypeOps(val tpe: TypeInformation[_]) extends AnyVal {
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
        case basicArray: BasicArrayTypeInfo[_, _] => Some(CTList(basicArray.getComponentInfo.toCypherType().get))
        case objArray: ObjectArrayTypeInfo[_, _] => Some(CTList(objArray.getComponentInfo.toCypherType().get))

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

    def toOrcType(): Option[TypeDescription] = {
      tpe match {
        case Types.STRING => Some(TypeDescription.createString())
        case Types.INT => Some(TypeDescription.createInt())
        case Types.LONG => Some(TypeDescription.createLong())
        case Types.BOOLEAN => Some(TypeDescription.createBoolean())
        case PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO => Some(TypeDescription.createList(TypeDescription.createBoolean()))
        case PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO => Some(TypeDescription.createList(TypeDescription.createDouble()))
        case PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO => Some(TypeDescription.createList(TypeDescription.createFloat()))
        case PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO => Some(TypeDescription.createList(TypeDescription.createInt()))
        case PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO => Some(TypeDescription.createList(TypeDescription.createLong()))
        case basicArray: BasicArrayTypeInfo[_, _] => Some(TypeDescription.createList(basicArray.getComponentInfo.toOrcType().get))
        case objArray: ObjectArrayTypeInfo[_, _] => Some(TypeDescription.createList(objArray.getComponentInfo.toOrcType().get))

        case _ => None
      }
    }

    def getOrcType: TypeDescription = toOrcType match {
      case Some(t) => t
      case None => throw NotImplementedException(s"mapping of $tpe to TypeDescription.")
    }
  }

  implicit class RecordHeaderOps(header: RecordHeader) extends Serializable {

    def toResolvedFieldReference: Seq[ResolvedFieldReference] = {
      header.columns.toSeq.sorted.map { column =>
        val expressions = header.expressionsFor(column)
        val commonType = expressions.map(_.cypherType).reduce(_ join _)
        assert(commonType.isFlinkCompatible,
          s"""
             |Expressions $expressions with common super type $commonType mapped to column $column have no compatible data type.
           """.stripMargin)
        ResolvedFieldReference(column, commonType.getFlinkType)
      }
    }
  }

  implicit class TableSchemaOps(val tableSchema: TableSchema) {
    def toRecordHeader: RecordHeader = {
      val exprToColumn = tableSchema.getFieldNames.map { columnName =>
        val cypherType = tableSchema.getFieldType(columnName).orElse(
          throw IllegalStateException(s"a missing TypeInformation for column $columnName")
        ).toCypherType() match {
          case Some(ct) => ct
          case None => throw IllegalArgumentException("a supported Flink type", tableSchema.getFieldType(columnName))
        }
        Var(columnName)(cypherType) -> columnName
      }

      RecordHeader(exprToColumn.toMap)
    }
  }
}