package org.opencypher.flink

import scala.annotation.tailrec

object StringEncodingUtilities {

  val propertyPrefix: String = "property#"

  protected  val maxCharactersInHexStringEncoding: Int = 4

  implicit class CharOps(val c: Char) extends AnyVal {
    def isAscii: Boolean = c.toInt <= 127
  }

  implicit class StringOps(val s: String) extends AnyVal {

    def toPropertyColumnName: String = {
      s"$propertyPrefix${s.encodeSpecialCharacters}"
    }

    def isProperyColumnName: Boolean = s.startsWith(propertyPrefix)

    def toProperty: String = {
      if (s.isProperyColumnName) {
        s.drop(propertyPrefix.length).decodeSpecialCharacters
      } else {
        s
      }
    }

    def encodeSpecialCharacters: String = {
      val sb = new StringBuilder

      @tailrec def recEncode(index: Int): Unit = {
        if (index < s.length) {
          val charToEncode = s(index)
          if ((charToEncode.isLetterOrDigit && charToEncode.isAscii) || charToEncode == '_'  || charToEncode == '#') {
            sb.append(charToEncode)
          } else {
            sb.append('@')
            val hexString = charToEncode.toHexString
            for(_ <- 0 until maxCharactersInHexStringEncoding - hexString.length) sb.append('0')
            sb.append(hexString)
          }
          recEncode(index + 1)
        }
      }

      recEncode(0)
      sb.toString
    }

    def decodeSpecialCharacters: String = {
      val sb = new StringBuilder

      @tailrec def recDecode(index: Int): Unit = {
        if (index < s.length) {
          val charToDecode = s(index)
          val nextIndex = if (charToDecode == '@') {
            val encodedHexStringStart = index + 1
            val indexAfterHexStringEnd = encodedHexStringStart + maxCharactersInHexStringEncoding
            val hexString = s.substring(encodedHexStringStart, indexAfterHexStringEnd)
            sb.append(hexString.parseHex)
            indexAfterHexStringEnd
          } else {
            sb.append(charToDecode)
            index + 1
          }
          recDecode(nextIndex)
        }
      }

      recDecode(0)
      sb.toString
    }

    def parseHex: Char = Integer.parseInt(s, 16).toChar
  }
}