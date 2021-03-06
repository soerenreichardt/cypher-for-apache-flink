package org.opencypher.flink.api.io.util

object FileSystemUtils {

  object FileSystemUtils {

    def using[T, U <: AutoCloseable](u: U)(f: U => T): T = {
      try {
        f(u)
      } finally {
        u.close()
      }
    }

  }
}
