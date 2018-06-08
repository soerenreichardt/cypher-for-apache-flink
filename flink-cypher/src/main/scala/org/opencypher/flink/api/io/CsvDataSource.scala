package org.opencypher.flink.api.io

import org.opencypher.flink.CAPFSession
import org.opencypher.flink.api.io.fs.FileBasedDataSource
import org.opencypher.flink.datasource.CAPFPropertyGraphDataSource

object CsvDataSource {

  def apply(rootPath: String)(implicit session: CAPFSession): CAPFPropertyGraphDataSource = {
    new FileBasedDataSource(rootPath, "csv")
  }

}
