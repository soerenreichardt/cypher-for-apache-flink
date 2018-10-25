package org.opencypher.flink.api.io.fs

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.{FileSystem, Path}
import org.opencypher.flink.api.io.util.FileSystemUtils._

object FlinkFSHelpers {

  implicit class RichFlinkFileSystem(fileSystem: FileSystem) {

    protected def createDirectoryIfNotExists(path: Path): Unit = {
      if (!fileSystem.exists(path)) {
        fileSystem.mkdirs(path)
      }
    }

    def listDirectories(path: String): List[String] = {
      val p = new Path(path)
      createDirectoryIfNotExists(p)
      fileSystem.listStatus(p)
        .filter(_.isDir)
        .map(_.getPath.getName)
        .toList
    }

    def deleteDirectory(path: String): Unit = {
      fileSystem.delete(new Path(path), /* recursive = */ true)
    }

    def readFile(path: String): String = {
      using(new BufferedReader(new InputStreamReader(fileSystem.open(new Path(path)), "UTF-8"))) { reader =>
        def readLines = Stream.cons(reader.readLine(), Stream.continually(reader.readLine))
        readLines.takeWhile(_ != null).mkString
      }
    }

    def writeFile(path: String, content: String): Unit = {
      val p = new Path(path)
      val parentDirectory = p.getParent
      createDirectoryIfNotExists(parentDirectory)
      using(fileSystem.create(p, WriteMode.OVERWRITE)) { outputStream =>
        using(new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))) { bufferedWriter =>
          bufferedWriter.write(content)
        }
      }
    }

  }

}
