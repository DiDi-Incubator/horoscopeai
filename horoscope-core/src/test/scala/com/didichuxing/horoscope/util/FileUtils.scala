package com.didichuxing.horoscope.util

import java.io.{File, IOException}
import java.nio.file.{FileVisitResult, FileVisitor, Files, Path}
import java.nio.file.attribute.BasicFileAttributes

import scala.collection.mutable.ListBuffer

object FileUtils {
  def walkFileTree(basePath: Path, endWith: Set[String]): Seq[File] = {
    val result: ListBuffer[File] = ListBuffer.empty
    Files.walkFileTree(basePath, new FileVisitor[Path] {
      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        FileVisitResult.CONTINUE
      }

      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        if (endWith.exists(file.toString.endsWith(_))) {
          result += file.toFile
        }
        FileVisitResult.CONTINUE
      }

      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
        FileVisitResult.CONTINUE
      }

      override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = {
        FileVisitResult.TERMINATE
      }
    })

    result
  }
}
