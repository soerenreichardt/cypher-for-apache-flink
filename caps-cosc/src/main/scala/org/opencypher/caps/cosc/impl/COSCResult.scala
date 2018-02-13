/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.caps.cosc.impl

import org.opencypher.caps.api.graph.{CypherResult, CypherResultPlan, Plan}
import org.opencypher.caps.cosc.impl.planning.COSCOperator
import org.opencypher.caps.impl.flat.FlatOperator
import org.opencypher.caps.impl.util.PrintOptions
import org.opencypher.caps.logical.impl.LogicalOperator

trait COSCResult extends CypherResult {

  override type LogicalPlan = LogicalOperator
  override type FlatPlan = FlatOperator
  override type PhysicalPlan = COSCOperator

  /**
    * The table of records that was returned by the query that produced this result.
    *
    * @return a table of records.
    */
  override def records: COSCRecords

  /**
    * The named graphs that were returned by the query that produced this result.
    *
    * @return a map of named graphs.
    */
  override def graphs: Map[String, COSCGraph]

  override def print(implicit options: PrintOptions): Unit = records.print

  override def toString: String = this.getClass.getSimpleName
}

object COSCResultBuilder {
  def from(logical: LogicalOperator, flat: FlatOperator, physical: COSCOperator)(implicit context: COSCRuntimeContext)
  : COSCResult = {
    new COSCResult {
      lazy val result: COSCPhysicalResult = physical.execute

      override def records: COSCRecords = result.records

      override def graphs: Map[String, COSCGraph] = result.graphs

      override def explain: Plan[LogicalOperator, FlatOperator, COSCOperator] = {
        Plan(CypherResultPlan(logical), CypherResultPlan(flat), CypherResultPlan(physical))
      }
    }
  }
}
