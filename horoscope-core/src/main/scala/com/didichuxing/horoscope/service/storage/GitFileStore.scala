/*
 * Copyright (C) 2021 DiDi Inc. All Rights Reserved.
 * Authors: liuhangfan_i@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.storage

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{entity, path, _}
import akka.http.scaladsl.server.{Route, StandardRoute}
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.service.storage.LocalFileStore.Node
import com.didichuxing.horoscope.util.GitUtil._
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config
import spray.json._

import java.io.File
import java.nio.file._
import scala.collection.JavaConversions.asScalaBuffer
import scala.util.Try

class GitFileStore(config: Config) extends LocalFileStore(config) with Logging {

  override val rootPath: String = config.getString("horoscope.storage.file-store.remote-root-path")
  val centralRepo: String = config.getString("horoscope.storage.file-store.remote-central-repo")
  val centralBranch: String =
    Try(config.getString("horoscope.storage.file-store.remote-central-branch")).getOrElse("master")

  private val defaultGitURL = "" // gitURL为空时, 默认指向centralRepo
  override def listFiles(gitURL: String): (String, Seq[File]) = {
    val pathname = getRelativePath(gitURL)
    super.listFiles(pathname)
  }

  import JsonSupport._

  override def api: Route = {
    concat(
      (post & path("read")) {
        fileReadRoute
      },
      (post & path("delete")) {
        fileDeleteRoute
      },
      (post & path("update")) {
        fileUpdateRoute
      },
      (put & path("rename")) {
        fileRenameRoute
      },
      (post & path("copy")) {
        fileCopyRoute
      },
      (get & path("tree")) {
        fileTreeRoute
      },
      (post & path("version")) {
        importGitRoute
      },
      (post & path("version" / "update")) {
        pullRoute
      },
      (put & path("version")) {
        pushRoute
      },
      (get & path("version" / "status")) {
        statusRoute
      },
      (get & path("version" / "diff")) {
        showDiffRoute
      },
      (delete & path("version" / "diff")) {
        resetDiffRoute
      },
      path("version" / "branches") {
        concat(
          get(listBranchesRoute),
          put(createBranchRoute),
          post(switchBranchRoute),
          delete(deleteBranchRoute)
        )
      },
      (get & path("version" / "log")) {
        commitLogRoute
      }
    )
  }

  val fileTreeRoute: Route = parameter("git") {
    git => {
      run {
        val segment = getPathname(git).split("/")
        walkFile(segment, prefix = segment.size)
      }
    }
  }

  val fileReadRoute: Route =
    entity(as[JsValue]) {
      json =>
        run {
          json.asJsObject.getFields("path", "gitURL", "branchName") match {
            case Seq(JsString(path), JsString(gitURL), JsString(branName)) =>
              getPathnameOnBranch(gitURL, branName, path) match {
                case Some(p) => {
                  try {
                    Files.readAllLines(Paths.get(p)).mkString("", "\n", "")
                  } catch {
                    case e: Exception => HttpResponse(400, entity = s"read failed")
                  }
                }
                case _ => HttpResponse(400, entity = s"on incorrect branch ${branName}")
              }
            case _ => HttpResponse(406, entity = s"lack parameters") //incorrect param
          }
        }
    }


  val fileDeleteRoute: Route =
    entity(as[JsValue]) {
      json =>
        run {
          json.asJsObject.getFields("path", "gitURL", "branchName") match {
            case Seq(JsString(path), JsString(gitURL), JsString(branName)) =>
              getPathnameOnBranch(gitURL, branName, path) match { //确保为当前branch，并返回本地上的文件夹地址
                case Some(p) => deleteFile(p)
                case _ => HttpResponse(400, entity = s"on incorrect branch ${branName}") //incorrect branch
              }
            case _ => HttpResponse(406, entity = s"lack parameters") //incorrect param
          }
        }
    }

  val fileUpdateRoute: Route =
    entity(as[JsValue]) {
      json =>
        run {
          json.asJsObject.getFields("path", "gitURL", "branchName", "isDir", "content") match {
            case Seq(JsString(path), JsString(gitURL), JsString(branName), JsBoolean(isDir), JsString(content)) =>
              getPathnameOnBranch(gitURL, branName, path) match {
                case Some(p) =>
                  if (content.isEmpty) {
                    if (isDir || validFile(p, fileType)) {   //文件类型合法检查
                      createFile(p, isDir)
                    } else {
                      false
                    }
                  } else {
                    try {
                      updateFile(p, content)
                    } catch {
                      case e: Exception =>
                        HttpResponse(400, entity = e.toString)
                    }
                  }
                case _ => HttpResponse(400, entity = s"on incorrect branch ${branName}")
              }
            case _ => HttpResponse(406, entity = s"lack parameters") //incorrect param
          }
        }
    }

  val fileRenameRoute: Route = parameter("git") {
    git => {
      entity(as[JsValue]) {
        json =>
          run {
            json.asJsObject.getFields("path", "branchName", "newName") match {
              case Seq(JsString(path), JsString(branName), JsString(newName)) =>
                getPathnameOnBranch(git, branName, path) match { //确保为当前branch，并返回本地上的文件夹地址
                  case Some(p) => renameFile(p, newName)
                  case _ => HttpResponse(406, entity = s"on incorrect branch $branName") //incorrect branch
                }
              case _ => HttpResponse(406, entity = s"lack parameters") //incorrect param
            }
          }
      }
    }
  }

  val fileCopyRoute: Route = parameter("git") {
    git => {
      entity(as[JsValue]) {
        json =>
          run {
            json.asJsObject.getFields("path", "branchName") match {
              case Seq(JsString(path), JsString(branName)) =>
                getPathnameOnBranch(git, branName, path) match { //确保为当前branch，并返回本地上的文件夹地址
                  case Some(p) => copyFile(p)
                  case _ => HttpResponse(400, entity = s"on incorrect branch $branName") //incorrect branch
                }
              case _ => HttpResponse(406, entity = s"lack parameters") //incorrect param
            }
          }
      }
    }
  }

  val importGitRoute: Route = entity(as[JsValue]) {
    json =>
      run {
        json.asJsObject.getFields("git", "userName", "password") match {
          case Seq(JsString(git), JsString(userName), JsString(password)) =>
            val gitURL = if (git.equals(defaultGitURL)) { //git参数为default指默认centralRepo
              centralRepo
            } else {
              s"https://$git"
            }
            val dir = getPathname(gitURL)
            try {
              val imported = importRepo(gitURL, dir, userName, password).getOrElse("")
              imported match {
                case true =>
                  setUpstream(getPathname(gitURL), centralRepo)
                  true
                case s:String =>
                  HttpResponse(400, entity = s)
              }
            } catch {
              case e: Exception =>
                deleteFile(dir)
                HttpResponse(400, entity = s"clone failed, ${e.toString}")
            }
          case _ => HttpResponse(406, entity = s"lack parameters") //incorrect param
        }
      }
  }

  val pullRoute: Route =
    entity(as[JsValue]) {
      json =>
        run {
          json.asJsObject.getFields("git", "userName", "password") match {
            case Seq(JsString(git), JsString(userName), JsString(password)) =>
              val dir = getPathname(git)
              val remote = getRemote(git)
              updateRepo(s"$dir/.git", centralBranch, remote, userName, password)
            case _ => HttpResponse(406, entity = s"lack parameters") //incorrect param
          }
        }
    }

  val pushRoute: Route = parameter("git") { git => {
    entity(as[JsValue]) { json =>
      run {
        json.asJsObject.getFields(
          "pushTarget", "commitMessage", "userName", "password", "branchName") match {
          case Seq(JsString(pushTarget), JsString(commitMessage), JsString(userName),
          JsString(password), JsString(branchName)) =>
            val dir = getPathname(git)
            var remote = "origin"
            var refSpec = branchName
            pushTarget match {
              case "upstream" =>
                remote = "upstream"
                refSpec = s"$branchName:refs/for/master"
              case "origin" =>
                if (branchName.equals("master")) {
                  refSpec = "master:refs/for/master"
                }
            }
            pushRepo(commitMessage, userName, password, dir.concat("/.git"), branchName, remote, refSpec)
          case _ => HttpResponse(406, entity = s"lack parameters")
        }
      }
    }
  }
  }

  val statusRoute: Route = parameter("git") {
    git =>
      run {
        statusRepo(getPathname(git))
      }
  }

  val showDiffRoute: Route = parameter("git") {
    git =>
      run {
        diffRepo(getPathname(git))
      }
  }

  val resetDiffRoute: Route = parameter("git") {
    git =>
      run {
        resetRepo(getPathname(git))
      }
  }

  val listBranchesRoute: Route = parameter("git") {
    git =>
      run {
        getBranches(getPathname(git))
      }
  }

  val switchBranchRoute: Route =
    parameter("git") {
      git => {
        entity(as[JsValue]) { json =>
          run {
            json.asJsObject.getFields("branchName") match {
              case Seq(JsString(branchName)) =>
                switchBranch(getPathname(git), branchName).getOrElse(false) match {
                  case true => true
                  case false => false
                  case s: String => HttpResponse(400, entity = s)
                }
              case _ => HttpResponse(406, entity = s"lack parameters") //incorrect param
            }
          }
        }
      }
    }

  val createBranchRoute: Route =
    parameter("git") {
      git => {
        entity(as[JsValue]) { json =>
          run {
            json.asJsObject.getFields("branchName") match {
              case Seq(JsString(branchName)) =>
                createBranch(getPathname(git), branchName).getOrElse(false) match {
                  case true => true
                  case false => false
                  case s: String => HttpResponse(400, entity = s)
                }
              case _ => HttpResponse(406, entity = s"lack parameters")
            }
          }
        }
      }
    }

  val deleteBranchRoute: Route =
    parameter("git") {
      git => {
        entity(as[JsValue]) { json =>
          run {
            json.asJsObject.getFields("branchName") match {
              case Seq(JsString(branchName)) =>
                deleteBranch(getPathname(git), branchName).getOrElse(false) match {
                  case true => true
                  case false => false
                  case s: String => HttpResponse(400, entity = s)
                }
              case _ => HttpResponse(406, entity = s"lack parameters") //incorrect param
            }
          }
        }
      }
    }

  val commitLogRoute: Route =
    parameter("git") {
      git =>
        run {
          logRepo(getPathname(git))
        }
    }

  import com.didichuxing.horoscope.runtime.Implicits._

  def run(block: Any): StandardRoute = {
    try {
      block match {
        case true => complete(StatusCodes.OK)
        case false => complete(StatusCodes.BadRequest)
        case s: String => complete(s)
        case b: Branches => complete(b.toJson)
        case n: Node => complete(Value(n))
        case h: HttpResponse => complete(h)
        case _ => complete(StatusCodes.BadRequest)
      }
    } catch {
      case e: Exception =>
        complete(StatusCodes.NotAcceptable, e.getMessage)
    }
  }

  def getRelativePath(gitURL: String): String = {
    val url = if (gitURL == defaultGitURL) centralRepo else gitURL
    val suffix = url.trim.split("/").takeRight(2)
    val userName = suffix(0)
    val projectName = suffix(1).substring(0, suffix(1).lastIndexOf("."))
    s"$userName/$projectName"
  }

  //根据git连接解析出文件(夹)绝对路径
  def getPathname(gitURL: String, relativePath: String = ""): String = {
    var pathname = ""
    if (gitURL.equals(defaultGitURL)) {
      pathname = gitURL2Workspace(centralRepo, rootPath)
    } else {
      pathname = gitURL2Workspace(gitURL, rootPath)
    }
    pathname.concat(relativePath)
  }

  //根据git链接解析出文件(夹)绝对路径，并验证当前Branch
  def getPathnameOnBranch(gitURL: String, branch: String, relativePath: String = ""): Option[String] = {
    var url = ""
    if (gitURL.equals(defaultGitURL)) {
      url = gitURL2Workspace(centralRepo, rootPath)
      Some(url.concat(relativePath))
    } else {
      url = gitURL2Workspace(gitURL, rootPath)
      if (branch == getBranch(url)) { // check is current branch
        Some(url.concat(relativePath))
      } else {
        None
      }
    }
  }

  def getRemote(gitURL: String): String = {
    if (gitURL.equals(defaultGitURL) || s"https://$gitURL".equals(centralRepo)) {
      "origin" // gitURL为空或主仓库
    } else {
      "upstream"
    }
  }

}

case class Branches(
   head: String = "",
   master: String = "",
   branch: Seq[String] = Seq.empty
)

object JsonSupport extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val branchFormat: JsonFormat[Branches] = jsonFormat3(Branches)
}
