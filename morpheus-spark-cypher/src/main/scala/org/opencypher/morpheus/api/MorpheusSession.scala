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
package org.opencypher.morpheus.api

import java.io.File
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.opencypher.morpheus.api.io._
import org.opencypher.morpheus.impl.graph.MorpheusGraphFactory
import org.opencypher.morpheus.impl.table.SparkTable.DataFrameTable
import org.opencypher.morpheus.impl.{MorpheusRecords, MorpheusRecordsFactory}
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.relational.api.graph.RelationalCypherSession
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult

import scala.reflect.runtime.universe._

/**
  * Spark-specific [[CypherSession]] implementation.
  *
  * This class is the main entry point for working with Morpheus. It wraps a [[SparkSession]] and allows
  * running Cypher queries over a set of distributed Spark [[DataFrame]]s.
  *
  * @param sparkSession The Spark session representing the cluster to execute on
  */
sealed class MorpheusSession(val sparkSession: SparkSession) extends RelationalCypherSession[DataFrameTable] with Serializable {

  override type Result = RelationalCypherResult[DataFrameTable]

  override type Records = MorpheusRecords

  protected implicit val morpheus: MorpheusSession = this

  override val records: MorpheusRecordsFactory = MorpheusRecordsFactory()

  override val graphs: MorpheusGraphFactory = MorpheusGraphFactory()

  override val elementTables: MorpheusElementTableFactory.type = MorpheusElementTableFactory
  /**
    * Reads a graph from sequences of nodes and relationships.
    *
    * @param nodes         sequence of nodes
    * @param relationships sequence of relationships
    * @tparam N node type implementing [[io.Node]]
    * @tparam R relationship type implementing [[io.Relationship]]
    * @return graph defined by the sequences
    */
  def readFrom[N <: Node : TypeTag, R <: Relationship : TypeTag](
    nodes: Seq[N],
    relationships: Seq[R] = Seq.empty
  ): PropertyGraph = {
    graphs.create(MorpheusNodeTable(nodes), MorpheusRelationshipTable(relationships))
  }

  def sql(query: String): MorpheusRecords =
    records.wrap(sparkSession.sql(query))
}

object MorpheusSession extends Serializable {

  /**
    * Creates a new [[MorpheusSession]] based on the given [[org.apache.spark.sql.SparkSession]].
    *
    * @return Morpheus session
    */
  def create(implicit sparkSession: SparkSession): MorpheusSession = new MorpheusSession(sparkSession)

  val localSparkConf: SparkConf = {
    val conf = new SparkConf(true)
    conf.set("spark.sql.codegen.wholeStage", "true")
    conf.set("spark.sql.shuffle.partitions", "12")
    conf.set("spark.default.parallelism", "8")
    // Required for left outer join without join expressions in OPTIONAL MATCH (leads to cartesian product)
    conf.set("spark.sql.crossJoin.enabled", "true")

    conf.set("spark.sql.orc.enableVectorizedReader", "true")

    // Store Hive tables in local temp folder
    conf.set("spark.sql.warehouse.dir", s"${System.getProperty("java.io.tmpdir")}${File.separator}spark-warehouse-${System.nanoTime()}")
    // Configure Hive to run with in-memory Derby (skips writing metastore_db)
    conf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:metastore_db;create=true")
    conf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
    conf
  }

  /**
    * Creates a new [[MorpheusSession]] that wraps a local Spark session with Morpheus default parameters.
    */
  def local(): MorpheusSession = {
    val session = SparkSession
      .builder()
      .config(localSparkConf)
      .master("local[*]")
      .appName(s"morpheus-local-${UUID.randomUUID()}")
      .enableHiveSupport()
      .getOrCreate()
    session.sparkContext.setLogLevel("error")

    create(session)
  }

  /**
    * Import this into scope in order to use:
    *
    * {{{
    * import org.opencypher.morpheus.api.MorpheusSession.RecordsAsDF
    * // ...
    * val df: DataFrame = results.records.asDF
    * }}}
    */
  // TODO is this still used?
  implicit class RecordsAsDF(val records: CypherRecords) extends AnyVal {
    /**
      * Extracts the underlying [[org.apache.spark.sql#DataFrame]] from the given [[records]].
      *
      * Note that the column names in the returned DF do not necessarily correspond to the names of the Cypher RETURN
      * items, e.g. "RETURN n.name" does not mean that the column for that item is named "n.name".
      *
      * @return [[org.apache.spark.sql#DataFrame]] representing the records
      */
    def asDataFrame: DataFrame = records match {
      case morpheus: MorpheusRecords => morpheus.table.df
      case _ => throw UnsupportedOperationException(s"can only handle Morpheus records, got $records")
    }

    /**
      * Converts all values stored in this table to instances of the corresponding CypherValue class.
      * In particular, this de-flattens, or collects, flattened elements (nodes and relationships) into
      * compact CypherNode/CypherRelationship objects.
      *
      * All values on each row are inserted into a CypherMap object mapped to the corresponding field name.
      *
      * @return [[org.apache.spark.sql.Dataset]] of CypherMaps
      */
    def asDataset: Dataset[CypherMap] = records match {
      case morpheus: MorpheusRecords => morpheus.toCypherMaps
      case _ => throw UnsupportedOperationException(s"can only handle Morpheus records, got $records")
    }
  }
}
