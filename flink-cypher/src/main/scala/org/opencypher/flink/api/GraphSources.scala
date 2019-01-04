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
package org.opencypher.flink.api

import org.opencypher.flink.api.io.{CsvFormat, OrcFormat, ParquetFormat}
import org.opencypher.flink.api.io.fs.FSGraphSource

object GraphSources {
  def fs(
    rootPath: String,
    filesPerTable: Option[Int] = Some(1)
  )(implicit session: CAPFSession) = FSGraphSources(rootPath, filesPerTable)
}

object FSGraphSources {
  def apply(
    rootPath: String,
    filesPerTable: Option[Int] = Some(1)
  )(implicit session: CAPFSession): FSGraphSourceFactory = FSGraphSourceFactory(rootPath, filesPerTable)

  case class FSGraphSourceFactory(
    rootPath: String,
    filesPerTable: Option[Int] = Some(1)
  )(implicit session: CAPFSession) {

    def csv: FSGraphSource = new FSGraphSource(rootPath, CsvFormat, filesPerTable)

    def parquet: FSGraphSource = new FSGraphSource(rootPath, ParquetFormat, filesPerTable)

    def orc: FSGraphSource = new FSGraphSource(rootPath, OrcFormat, filesPerTable)
  }
}
