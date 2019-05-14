/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.morpheus.util

import java.util.Calendar

import org.apache.spark.sql.types.{StringType, StructField, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Properties

object LdbcUtil {

  // Those tables are not used during Graph Ddl generation
  val excludeTables: Set[String] = Set(
    // normalized node tables
    "place",
    "organisation",
    // normalized relationship tables
    "person_islocatedin_place",
    "comment_islocatedin_place",
    "post_islocatedin_place",
    "organisation_islocatedin_place",
    "place_ispartof_place",
    "person_studyat_organisation",
    "person_workat_organisation",
    // list properties
    "person_email_emailaddress",
    "person_speaks_language")

  case class NodeType(nodeTable: String) {
    override def toString: String = s"(${nodeTable.toNodeLabel})"
  }

  case class EdgeType(startNodeType: NodeType, edgeType: String, endNodeType: NodeType) {
    def edgeTable: String = s"${startNodeType.nodeTable}_${edgeType}_${endNodeType.nodeTable}"
    override def toString: String = s"$startNodeType-[$edgeType]->$endNodeType"
  }

  /**
    * Generates a Graph Ddl script for the LDBC graph. The extraction of nodes and relationship types and their
    * corresponding table/view mappings is based on the naming conventions used by the LDBC data generator.
    *
    * @param datasource the data source used in the SET SCHEMA definition
    * @param database   the Hive database to get the table information from
    * @param spark      a Spark instance
    * @return a Graph Ddl string
    */
  def toGraphDDL(datasource: String, database: String)(implicit spark: SparkSession): String = {
    // select all tables (including views)
    val tableNames = spark.sql(s"SHOW TABLES FROM $database").collect()
      .filterNot(_.getBoolean(2)) //ignore temp tables
      .map(_.getString(1))
      .filterNot(excludeTables.contains)
      .toSet

    val nodeTables = tableNames.filterNot(_.contains("_"))
    val edgeTables = tableNames -- nodeTables

    val elementTypes = toElementType(database, nodeTables ++ edgeTables).toSeq.sorted

    val nodeMappings = nodeTables.map(nodeTable => s"(${nodeTable.toNodeLabel}) FROM $nodeTable")
    val edgeMappings = edgeTables
      .map(_.split("_"))
      .map(tokens => EdgeType(NodeType(tokens(0)), tokens(1), NodeType(tokens(2))))
      .map {
        case et@EdgeType(snt@NodeType(startNodeTable), _, ent@NodeType(endNodeTable)) =>

          val startJoinExpr = if (startNodeTable == endNodeTable) {
            s"edge.$startNodeTable.id0 = node.id"
          } else {
            s"edge.$startNodeTable.id = node.id"
          }

          val endJoinExpr = if (startNodeTable == endNodeTable) {
            s"edge.$endNodeTable.id1 = node.id"
          } else {
            s"edge.$endNodeTable.id = node.id"
          }

          val startNodes = s"$snt FROM $startNodeTable node JOIN ON $startJoinExpr"
          val endNodes = s"$ent FROM $endNodeTable node JOIN ON $endJoinExpr"

          et -> s"FROM ${et.edgeTable} edge START NODES $startNodes END NODES $endNodes"
      }
      .groupBy(_._1)
      .map { case (relType, mappings) =>
        val fromClauses = mappings.map(_._2)
        s"""$relType
           |${fromClauses.mkString("\t\t", Properties.lineSeparator + "\t\t", "")}""".stripMargin
      }

    s"""-- generated by ${getClass.getSimpleName.dropRight(1)} on ${Calendar.getInstance().getTime}
       |
       |SET SCHEMA $datasource.$database
       |
       |${elementTypes.mkString(Properties.lineSeparator)}
       |
       |CREATE GRAPH $database (
       |    -- Node types including mappings
       |    ${nodeMappings.mkString("", "," + Properties.lineSeparator + "\t", ",")}
       |
       |    -- Edge types including mappings
       |    ${edgeMappings.mkString("," + Properties.lineSeparator + "\t")}
       |)
       """.stripMargin
  }

  private def toElementType(database: String, tableNames: Set[String])(implicit spark: SparkSession): Set[String] =
    tableNames.map {
      tableName =>
        val tableInfos = spark.sql(s"DESCRIBE TABLE $database.$tableName").collect()

        val label = if (tableName.contains("_")) {
          tableName.split("_")(1)
        } else {
          tableName.toNodeLabel
        }
        val properties = tableInfos
          .filterNot(row => row.getString(0) == "id" || row.getString(0).contains(".id"))
          .map { row =>
            val propertyKey = row.getString(0)
            val propertyType = row.getString(1).toCypherType
            s"$propertyKey $propertyType"
          }

        if (properties.nonEmpty) {
          s"CREATE ELEMENT TYPE $label ( ${properties.mkString(", ")} )"
        } else {
          s"CREATE ELEMENT TYPE $label"
        }
    }

  implicit class StringHelpers(val s: String) extends AnyVal {

    def toNodeLabel: String = s.capitalize

    def toCypherType: String = s.toUpperCase match {
      case "STRING" => "STRING"
      case "BIGINT" => "INTEGER"
      case "INT" => "INTEGER"
      case "BOOLEAN" => "BOOLEAN"
      case "FLOAT" => "FLOAT"
      case "DOUBLE" => "FLOAT"
      // TODO: map correctly as soon as we support timestamp
      case "TIMESTAMP" => "STRING"
    }
  }

  implicit class DataFrameConversion(df: DataFrame) {

    def withCompatibleTypes: DataFrame = df.schema.fields.foldLeft(df) {
      case (currentDf, StructField(name, dataType, _, _)) if dataType == TimestampType =>
        currentDf
          .withColumn(s"${name}_tmp", currentDf.col(name).cast(StringType))
          .drop(name)
          .withColumnRenamed(s"${name}_tmp", name)

      case (currentDf, _) => currentDf
    }
  }
}