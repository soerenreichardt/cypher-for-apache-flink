package org.opencypher.flink.impl

import org.opencypher.okapi.relational.impl.table.CharacterEscaping

class CAPFCharacterEscaping extends CharacterEscaping {
  override def replaceCharactersInColumnName(columnName: String): String = {
    columnName
      .replaceAll("-", "_")
      .replaceAll(":", "_")
      .replaceAll("\\.", "_")
      .replace("(", "_")
      .replace(")", "_")
  }

}
