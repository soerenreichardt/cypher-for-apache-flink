package org.opencypher.flink.io

import io.circe.Decoder
import io.circe.generic.auto._
import io.circe.parser.parse
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, TypeInformation}
import org.apache.flink.table.api.Types

abstract class CsvSchema {
  def idField: CsvField
  def propertyFields: List[CsvField]

}

object CsvSchemaUtils {
  def parseJson[T](jsonString: String)(implicit decoder: Decoder[T]): T = {
    parse(jsonString) match {
      case Left(failure) => throw new RuntimeException(s"Invalid json file: $failure")
      case Right(json) =>
        json.hcursor.as[T] match {
          case Left(failure) => {
            val msg =
              s"Invalid JSON schema: Could not find mandatory element '${failure.history.head.productElement(0)}'"
            throw new RuntimeException(msg)
          }
          case Right(elem) => elem
        }
    }
  }
}

case class CsvField(name: String, column: Int, valueType: String) {
  private val listType = raw"list\[(\w+)\]".r

  lazy val getSourceType: TypeInformation[_] = valueType.toLowerCase match {
    case l if listType.pattern.matcher(l).matches() => Types.STRING
    case other => extractSimpleType(other)
  }

  lazy val getTargetType: TypeInformation[_] = valueType.toLowerCase match {
    case l if listType.pattern.matcher(l).matches() => l match {
      case listType(inner) if inner == "string" => BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO//Types.OBJECT_ARRAY(extractSimpleType(inner))
      case listType(inner) => Types.PRIMITIVE_ARRAY(extractSimpleType(inner))
    }
    case other => extractSimpleType(other)
  }

  private def extractSimpleType(typeString: String): TypeInformation[_] = typeString match {
    case "string"     => Types.STRING
    case "integer"    => Types.INT
    case "long"       => Types.LONG
    case "boolean"    => Types.BOOLEAN
    case "float"      => Types.FLOAT
    case "double"     => Types.DOUBLE
    case x            => throw new RuntimeException(s"Unknown type $x")
  }
}

case class CsvNodeSchema(
  idField: CsvField,
  implicitLabels: List[String],
  optionalLabels: List[CsvField],
  propertyFields: List[CsvField])
  extends CsvSchema {

  def names: Array[String] = {
    (List(idField) ++ optionalLabels ++ propertyFields)
      .sortBy(_.column)
      .map(_.name)
      .toArray
  }

  def types: Array[TypeInformation[_]] = {
    (List(idField) ++ optionalLabels ++ propertyFields)
      .sortBy(_.column)
      .map(_.getSourceType)
      .toArray
  }

}

object CsvNodeSchema {
  implicit val decodeNodeCsvSchema: Decoder[CsvNodeSchema] = for {
    idField <- Decoder.instance(_.get[CsvField]("idField"))
    implicitLabels <- Decoder.instance(_.get[List[String]]("implicitLabels"))
    optionalLabels <- Decoder.instance(_.getOrElse[List[CsvField]]("optionalLabels")(List()))
    propertyFields <- Decoder.instance(_.getOrElse[List[CsvField]]("propertyFields")(List()))
  } yield new CsvNodeSchema(idField, implicitLabels, optionalLabels, propertyFields)

  def apply(schemaJson: String): CsvNodeSchema = {
    CsvSchemaUtils.parseJson(schemaJson)
  }
}

case class CsvRelSchema(
  idField: CsvField,
  startIdField: CsvField,
  endIdField: CsvField,
  relType: String,
  propertyFields: List[CsvField])
  extends CsvSchema {

  def names: Array[String] = {
    (List(idField, startIdField, endIdField) ++ propertyFields)
      .sortBy(_.column)
      .map(_.name)
      .toArray
  }

  def types: Array[TypeInformation[_]] = {
    (List(idField, startIdField, endIdField) ++ propertyFields)
      .sortBy(_.column)
      .map(_.getSourceType)
      .toArray
  }
}

object CsvRelSchema {
  implicit val decodeRelCsvSchema: Decoder[CsvRelSchema] = for {
    id <- Decoder.instance(_.get[CsvField]("idField"))
    startIdField <- Decoder.instance(_.get[CsvField]("startIdField"))
    endIdField <- Decoder.instance(_.get[CsvField]("endIdField"))
    relType <- Decoder.instance(_.get[String]("relationshipType"))
    propertyFields <- Decoder.instance(_.getOrElse[List[CsvField]]("propertyFields")(List()))
  } yield new CsvRelSchema(id, startIdField, endIdField, relType, propertyFields)

  def apply(schemaJson: String): CsvRelSchema = {
    CsvSchemaUtils.parseJson(schemaJson)
  }
}
