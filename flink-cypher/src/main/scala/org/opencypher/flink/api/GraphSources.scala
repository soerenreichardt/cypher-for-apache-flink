package org.opencypher.flink.api

import org.opencypher.flink.api.io.{CsvFormat, OrcFormat}
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
    filesPerTable: Option[Int]
  )(implicit session: CAPFSession): FSGraphSourceFactory = FSGraphSourceFactory(rootPath, filesPerTable)

  case class FSGraphSourceFactory(
    rootPath: String,
    filesPerTable: Option[Int] = Some(1)
  )(implicit session: CAPFSession) {

    def csv: FSGraphSource = new FSGraphSource(rootPath, CsvFormat, filesPerTable)

    def orc: FSGraphSource = new FSGraphSource(rootPath, OrcFormat, filesPerTable)
  }
}
