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
package org.opencypher.flink.api

import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.calcite.{CalciteConfig, CalciteConfigBuilder}
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.opencypher.flink.api.io.CAPFElementTableFactory
import org.opencypher.flink.impl.graph.CAPFGraphFactory
import org.opencypher.flink.impl.table.FlinkCypherTable.FlinkTable
import org.opencypher.flink.impl.{CAPFRecords, CAPFRecordsFactory}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.relational.api.graph.RelationalCypherSession
import org.opencypher.okapi.relational.api.planning.RelationalCypherResult

import scala.collection.JavaConverters._

sealed class CAPFSession private(
  val env: ExecutionEnvironment,
  val tableEnv: BatchTableEnvironment
) extends RelationalCypherSession[FlinkTable] with Serializable {

  override type Result = RelationalCypherResult[FlinkTable]

  override type Records = CAPFRecords

  protected implicit val capf: CAPFSession = this

  override val records: CAPFRecordsFactory = CAPFRecordsFactory()

  override val graphs: CAPFGraphFactory = CAPFGraphFactory()

  override val elementTables: CAPFElementTableFactory = CAPFElementTableFactory(capf)

  def sql(query: String): CAPFRecords =
    records.wrap(tableEnv.sqlQuery(query))
}

object CAPFSession extends Serializable {

  private val deactivatedLogicalRules = Seq(
//    "ProjectMergeRule:force_mode"
  )

  def create(implicit env: ExecutionEnvironment): CAPFSession = {
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    tableEnv.config.setCalciteConfig(configureCalcite(tableEnv.config.getCalciteConfig))
    new CAPFSession(env, tableEnv)
  }

  def local(): CAPFSession = create(ExecutionEnvironment.getExecutionEnvironment)

  private def configureCalcite(config: CalciteConfig): CalciteConfig = {
    val logicalRules = FlinkRuleSets.LOGICAL_OPT_RULES
    val filteredRules = logicalRules.iterator().asScala.filterNot(rule => deactivatedLogicalRules.contains(rule.toString)).toList

    config.replacesDecoRuleSet
    new CalciteConfigBuilder()
      .replaceLogicalOptRuleSet(RuleSets.ofList(filteredRules: _*))
      .build()
  }

  implicit class RecordsAsTable(val records: CypherRecords) extends AnyVal {

    def asTable: Table = records match {
      case capf: CAPFRecords => capf.table.table
      case _ => throw UnsupportedOperationException(s"can only handle CAPF records, got $records")
    }

    def asDataSet: DataSet[CypherMap] = records match {
      case capf: CAPFRecords => capf.toCypherMaps
      case _ => throw UnsupportedOperationException(s"can only handle CAPF records, got $records")
    }
  }

}