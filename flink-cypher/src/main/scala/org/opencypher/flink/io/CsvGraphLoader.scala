package org.opencypher.flink.io

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.stream.Collectors

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.table.api.scala.array
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.expressions.UnresolvedFieldReference
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row
import org.opencypher.flink.{CAPFSession, ColumnName}
import org.opencypher.flink.TableOps._
import org.opencypher.flink.schema.{CAPFNodeTable, CAPFRelationshipTable}
import org.opencypher.flink.schema.EntityTable._
import org.opencypher.flink.FlinkUtils._
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.impl.exception.IllegalArgumentException

trait CsvGraphLoaderFileHandler {
  def location: String

  def listDataFiles(directory: String): Array[URI]

  def readSchemaFile(path: URI): String
}

final class LocalFileHandler(override val location: String) extends CsvGraphLoaderFileHandler {

  import scala.collection.JavaConverters._

  override def listDataFiles(directory: String): Array[URI] = {
    Files
      .list(Paths.get(s"$location${File.separator}$directory".substring(1)))
      .collect(Collectors.toList())
      .asScala
      .filter(p => p.toString.endsWith(".csv") | p.toString.endsWith(".CSV"))
      .toArray
      .map(_.toUri)
  }

  override def readSchemaFile(csvPath: URI): String = {
    val schemaPaths = Seq(
      new URI(s"${csvPath.toString}.schema"),
      new URI(s"${csvPath.toString}.SCHEMA")
    )

    val optSchemaPath = schemaPaths.find(p => new File(p).exists())
    val schemaPath = optSchemaPath.getOrElse(throw IllegalArgumentException(s"Could not find schema file at $csvPath"))
    new String(Files.readAllBytes(Paths.get(schemaPath)))
  }
}

class CsvGraphLoader(fileHandler: CsvGraphLoaderFileHandler)(implicit capf: CAPFSession) {

  def load: PropertyGraph = capf.readFrom(loadNodes ++ loadRels: _*)

  private def loadNodes: List[CAPFNodeTable] = {
    val csvFiles = listCsvFiles("nodes").toList

    csvFiles.map { e =>
      val schema = parseSchema(e)(CsvNodeSchema(_))

      val csvSource = new CsvTableSource(e.getRawPath, schema.names, schema.types)
      capf.tableEnv.registerTableSource(e.toString, csvSource)

      val intermediateTable = capf.tableEnv.scan(e.toString)

      val table = convertLists(intermediateTable, schema)

      val nodeMapping = NodeMapping.create(schema.idField.name,
        impliedLabels = schema.implicitLabels.toSet,
        optionalLabels = schema.optionalLabels.map(_.name).toSet,
        propertyKeys = schema.propertyFields.map(_.name).toSet)

      CAPFNodeTable.fromMapping(nodeMapping, table)
    }
  }

  private def loadRels: List[CAPFRelationshipTable] = {
    val csvFiles = listCsvFiles("relationships").toList

    csvFiles.map(e => {

      val schema = parseSchema(e)(CsvRelSchema(_))

      val csvSource = new CsvTableSource(e.getRawPath, schema.names, schema.types)
      capf.tableEnv.registerTableSource(e.toString, csvSource)

      val intermediateTable = capf.tableEnv.scan(e.toString)

      val table = convertLists(intermediateTable, schema)

      val relMapping = RelationshipMapping.create(schema.idField.name,
        schema.startIdField.name,
        schema.endIdField.name,
        schema.relType,
        schema.propertyFields.map(_.name).toSet
      )

      CAPFRelationshipTable.fromMapping(relMapping, table)
    })
  }

  private def listCsvFiles(directory: String): Array[URI] =
    fileHandler.listDataFiles(directory)

  private def parseSchema[T <: CsvSchema](path: URI)(parser: String => T): T = {
    val text = fileHandler.readSchemaFile(path)
    parser(text)
  }

  private def convertLists(table: Table, schema: CsvSchema): Table = {
    schema.propertyFields
      .filter(field => field.getTargetType.isInstanceOf[PrimitiveArrayTypeInfo[_]] ||
        field.getTargetType.isInstanceOf[ObjectArrayTypeInfo[_, _]] ||
        field.getTargetType.isInstanceOf[BasicArrayTypeInfo[_, _]]
      )
      .foldLeft(table) {
        case (t, field) =>
          val split = new Split("\\|")
          val tempIdField = "_tmp_id"
          val splitColumn = t.select(
            UnresolvedFieldReference(schema.idField.name) as Symbol(tempIdField),
            split(UnresolvedFieldReference(field.name)) as Symbol(field.name))

          t
            .safeDropColumn(field.name)
            .safeJoin(splitColumn, Seq((schema.idField.name, tempIdField)), "inner")
            .safeDropColumn(tempIdField)
      }
  }

}

class Split(separator: String) extends ScalarFunction {

  def eval(str: String): Array[String] = {
    str.split(separator)
  }

}

object CsvGraphLoader {
  def apply(location: String)(implicit capf: CAPFSession): CsvGraphLoader =
    new CsvGraphLoader(new LocalFileHandler(location))
}
