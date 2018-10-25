package org.opencypher.flink.api.io

trait StorageFormat {
  def name: String = getClass.getSimpleName.dropRight(7).toLowerCase
}

case object CsvFormat extends StorageFormat

case object AvroFormat extends StorageFormat

case object ParquetFormat extends StorageFormat

case object OrcFormat extends StorageFormat

case object JdbcFormat extends StorageFormat

case object HiveFormat extends StorageFormat

case object Neo4jFormat extends StorageFormat
