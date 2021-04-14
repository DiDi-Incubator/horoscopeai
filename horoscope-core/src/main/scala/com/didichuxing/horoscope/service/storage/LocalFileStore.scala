/*
 * Copyright (C) 2021 DiDi Inc. All Rights Reserved.
 * Authors: liuhangfan_i@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.storage

import com.didichuxing.horoscope.core.FlowDslMessage.CompositorDef
import com.didichuxing.horoscope.core.{Compositor, FileStore, Flow}
import com.didichuxing.horoscope.dsl.FlowCompiler
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, SimpleBuiltIn}
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config

import java.io.{BufferedWriter, File, IOException}
import java.nio.charset.Charset
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.Try

class LocalFileStore(config: Config) extends FileStore with Logging {
  import LocalFileStore._

  val rootPath: String = Try(config.getString("horoscope.storage.file-store.local-root-path")).getOrElse("")
  val fileType: List[String] = Try(config.getStringList("horoscope.storage.file-store.type").toList)
    .getOrElse(Nil)
  val ignoreFiles: List[String] = Try(config.getStringList("horoscope.storage.file-store.ignore-files").toList)
    .getOrElse(Nil)

  /**
   * @param path : pathname string
   * @return file instance
   */
  override def getFile(path: String): File = {
    Paths.get(path).toFile
  }

  /**
   * Return a list of Files by walking the file tree rooted at the given starting file with depth-first method.
   * The returned file list is filtered and guaranteed to have at least one element, the starting file itself.
   *
   * @param username : owner of this repository
   * @return workspace, a List of files
   */
  override def listFiles(username: String = ""): (String, Seq[File]) = {
    val list = new ListBuffer[File]
    val pathname = rootPath.concat("/").concat(username)
    try {
      Files.walk(Paths.get(pathname))
        .iterator()
        .filter(p => validFile(p.toString, fileType,ignoreFiles))
        .foreach(p => list.append(p.toFile))
    } catch {
      case e: Exception =>
        logging.error(s"fail to list files under $pathname", e)
    }
    (pathname, list.toList)
  }

  /**
   * Return a node which denotes directory tree by walking at the giving file path
   * The file tree is traversed depth-first
   *
   * @param segments : pathname string, denotes file or directory
   * @return Node
   */
  def walkFile(segments: Seq[String] = Nil, prefix: Int = 0): Node = {
    val path = segments.mkString("", "/", "")
    val p = Paths.get(path)
    val children = ListBuffer.empty[Node]
    if (Files.isDirectory(p)) {
      try {
        Files.walk(p, 1)
          .skip(1) //skip itself
          .iterator()
          .filter(p => if(Files.isDirectory(p)) !p.getFileName.toString.startsWith(".") else true)
          .filter(p => validFile(p.toString, fileType, ignoreFiles))
          .foreach {
            child =>
              children.append(walkFile(segments :+ child.getFileName.toString, prefix))
          }
      } catch {
        case e: Exception =>
          logging.error(s"fail to walk $path", e)
      }
    }
    val pathname = segments.drop(prefix).mkString("/", "/", "")
    Node(name = segments.lastOption.getOrElse("/"), path = pathname,
      children = children.toList)
  }

  /**
   * Update a file with content, create the file if not exists
   *
   * @param path    pathname of the target file
   * @param content what to write
   * @return has updated or not
   */
  override def updateFile(path: String, content: String): Boolean = {
    var isUpdated = false
    val target = Paths.get(path)
    if (Files.notExists(target)) Files.createFile(target)
    var writer: BufferedWriter = null
    try {
      if (path.endsWith(".flow")) {
        require(path.endsWith(checkSyntax(content).name + ".flow"), "wrong flow name in header")
      }
      writer = Files.newBufferedWriter(target, Charset.forName("UTF-8"))
      writer.write(content)
      isUpdated = true
    }finally {
      if (writer != null) writer.close()
    }
    isUpdated
  }

  /**
   * create an empty file or directory with the giving pathname
   *
   * @param path        pathname
   * @param isDirectory is file or directory
   * @return has created or not
   */
  override def createFile(path: String, isDirectory: Boolean): Boolean = {
    val target = Paths.get(path)
    if (Files.notExists(target)) {
      if (isDirectory) {
        Files.createDirectories(target)
      } else {
        Files.createFile(target)
      }
      true
    } else {
      false
    }
  }


  //delete the file or directory with depth-first
  override def deleteFile(path: String): Boolean = {
    var done = false
    val target = Paths.get(path)
    val exists: Boolean = Files.exists(target)
    if (exists) {
      try {
        Files.walkFileTree(target, new SimpleFileVisitor[Path]() {
          @throws(classOf[IOException])
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.delete(file)
            if (file.equals(target)) done = true
            FileVisitResult.CONTINUE
          }

          @throws(classOf[IOException])
          override def postVisitDirectory(dir: Path, e: IOException): FileVisitResult = {
            Files.delete(dir)
            if (dir.equals(target)) done = true
            FileVisitResult.CONTINUE
          }
        })
      } catch {
        case e: Exception =>
          logging.error(s"fail to delete $path", e)
      }
    }
    done
  }

  /**
   * copy a file to the current directory. The output file name with suffix "_copy"
   *
   * @param path pathname
   * @return has copied or not
   */
  override def copyFile(path: String): Boolean = {
    var done = false
    val source = Paths.get(path)
    if (Files.isRegularFile(source)) {
      try {
        val oldName = source.getFileName.toString
        val i = oldName.lastIndexOf(".")
        val newName = new StringBuffer(oldName)
        if (i != -1) {
          newName.insert(i, "_copy")
        } else {
          newName.append("_copy")
        }
        val target = source.resolveSibling(newName.toString)
        Files.copy(source, target)
        if (Files.exists(target)) done = true
      } catch {
        case e: Exception =>
          logging.error(s"fail to copy $path", e)
      }
    }
    done
  }

  /**
   * rename file with a giving name
   *
   * @param path pathname
   * @param name new name
   * @return has renamed or not
   */
  override def renameFile(path: String, name: String): Boolean = {
    var done = false
    val source = Paths.get(path)
    val target = source.resolveSibling(name)
    if (Files.exists(source)) {
      try {
        Files.move(source, target)
        done = true
      } catch {
        case e: Exception =>
          logging.error(s"fail to rename $path", e)
      }
    }
    done
  }

  //create empty directory with a giving pathname
  def createDirectories(path: String): Boolean = {
    var done = false
    val target = Paths.get(path)
    if (Files.notExists(target)) {
      try {
        Files.createDirectories(target)
        done = true
      } catch {
        case e: Exception =>
          logging.error(s"fail to create $path", e)
      }
    }
    done
  }

  def validFile(path: String, validType: List[String], ignoreFiles: List[String] = Nil): Boolean = {
    val p = Paths.get(path)
    if(ignoreFiles.contains(p.getFileName.toString)) {
      false
    }
    else {
      if(Files.isDirectory(p)) true else validType.contains(path.split('.').lastOption.getOrElse(""))
    }
  }

}

object LocalFileStore{
  case class Node(name: String, path: String, children: Seq[Node])

  def checkSyntax(text: String): Flow = {
    implicit def buildCompositor(flow: String, compositorDef: CompositorDef): Compositor = new Compositor {
      def composite(args: ValueDict): Future[Value] = {
        Future.failed(new NotImplementedError())
      }
    }
    // scalastyle:off
    implicit val builtin: BuiltIn = new SimpleBuiltIn(Map.empty, Map.empty)

    Flow(FlowCompiler.compile(text))
  }
}
