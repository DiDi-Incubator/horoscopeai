/*
 * Copyright (C) 2021 DiDi Inc. All Rights Reserved.
 * Authors: liuhangfan_i@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.util

import com.didichuxing.horoscope.service.storage.Branches
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.transport.{PushResult, RefSpec, UsernamePasswordCredentialsProvider}

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.sys.process.{Process, ProcessLogger}

object GitUtil extends Logging {
  def importRepo(gitURL: String, repoDir: String): Boolean = {
    var done = false
    if (gitURL.endsWith(".git")) {
      try {
        val dir = Files.createDirectories(Paths.get(repoDir))
        if (dir.toFile.listFiles().isEmpty) {
          Git.cloneRepository.setURI(gitURL).setDirectory(dir.toFile).call
        }
        done = true
      } catch {
        case e: Exception =>
          logging.error("fail to import", e)
      }
    }
    done
  }

  //设置upstream仓库
  def setUpstream(path: String, gitURL: String): Boolean = {
    var done = false
    val commandLogger = GitCommandLogger()
    try {
      val exitCode = Process(Seq("git", "remote", "add", "upstream", gitURL), new File(path))
        .!(commandLogger.gitCommandLogger)
      if (exitCode == 0) {
        done = true
      }
    } catch {
      case e: Exception =>
        logging.error(s"fail to set upstream in $path", e)
    }
    done
  }

  def updateRepo(path: String, remoteBranch: String, remote: String): String = {
    val message = new StringBuffer()
    val commandLogger = GitCommandLogger()
    try {
      val exitCode = Process(Seq("git", "pull", remote, remoteBranch, "--stat"), new File(path))
        .!(commandLogger.gitCommandLogger)
      exitCode match {
        case 0 => message.append(commandLogger.errorResult.toList.mkString("", "\n", "\n"))
          .append(commandLogger.outputResult.toList.mkString("", "\n", ""))
        case _ => message.append(commandLogger.errorResult.toList.mkString("", "\n", ""))
      }
    } catch {
      case e: Exception =>
        logging.error(s"fail to update in $path", e)
    }
    message.toString
  }


  def pushRepo(message: String, name: String, pw: String, path: String): String = {
    val res = new StringBuffer()
    try {
      val repo = new FileRepository(path)
      val git: Git = new Git(repo)
      val branch = repo.getBranch
      git.add.addFilepattern(".").call
      val commitRes: RevCommit = git.commit.setMessage(message).call
      res.append(s"[$branch ${commitRes.name().substring(0, 8)}] ")
      res.append(s"${commitRes.getShortMessage} \n")
      res.append(s"Committer: ${commitRes.getCommitterIdent.getName} \n")
      val push = git.push.setRemote("origin")
        .setForce(true)
        .setCredentialsProvider(new UsernamePasswordCredentialsProvider(name, pw))
      if (branch.equals("master")) {
        push.setRefSpecs(new RefSpec(s"master:refs/for/master"))
      } else {
        push.setRefSpecs(new RefSpec(branch))
      }
      val pushResults = push.call()
      pushResults.asScala.foreach {
        push: PushResult => {
          res.append(push.getMessages)
          push.getTrackingRefUpdates.asScala.foreach {
            up => {
              Option(up.getOldObjectId) match {
                case Some(o) => res.append(o.abbreviate(7).name)
                case None => res.append(" ")
              }
              res.append("..")
              Option(up.getNewObjectId) match {
                case Some(o) => res.append(o.abbreviate(7).name)
                case None => res.append(" ")
              }
              res.append(s"${up.getLocalName} -> ${up.getRemoteName}")
            }
          }
        }
      }
      res.toString
    } catch {
      case e: Exception =>
        logging.error(s"fail to push $path", e.printStackTrace())
        s"fail to push $path"
    }
  }


  def diffRepo(path: String): String = {
    var diff = ""
    val commandLogger = GitCommandLogger()
    val file = new File(path)
    try {
      if (file.exists()) {
        Process("git add .", file).!!(commandLogger.gitCommandLogger)
        diff = Process(Seq("git", "diff", "HEAD"), file).!!(commandLogger.gitCommandLogger)
      }
    } catch {
      case e: Exception =>
        logging.error(s"fail to show diff in $path", e)
    }
    diff
  }

  def resetRepo(path: String): Boolean = {
    var done: Boolean = false
    val commandLogger = GitCommandLogger()
    try {
      val exitCode = Process("git reset --hard", new File(path)).!(commandLogger.gitCommandLogger)
      if (exitCode == 0) done = true
    } catch {
      case e: Exception =>
        logging.error(s"fail to rest $path", e)
    }
    done
  }

  def statusRepo(path: String): String = {
    var status = ""
    val commandLogger = GitCommandLogger()
    try {
      val file = new File(path)
      val toStage = Process("git add .", file).!(commandLogger.gitCommandLogger)
      toStage match {
        case 0 =>
          val exitCode = Process("git status", file).!(commandLogger.gitCommandLogger)
          exitCode match {
            case 0 => status = commandLogger.outputResult.toList.mkString("", "\n", "\n")
            case _ => status = commandLogger.errorResult.toList.mkString("", "\n", "\n")
          }
        case _ => status = commandLogger.errorResult.toList.mkString("", "\n", "\n")
      }
    } catch {
      case e: Exception =>
        logging.error(s"fail to list status of $path", e)
    }
    status
  }

  def logRepo(path: String): String = {
    val commandLogger = GitCommandLogger()
    try {
      Process(Seq("git", "log", "--stat", "--graph", "--pretty=format:\"%h %s by %an since %cr\"", "--decorate"),
        new File(path)).!!(commandLogger.gitCommandLogger)
    } catch {
      case e: Exception =>
        logging.error(s"fail to list log of $path", e)
        null
    }
  }

  def getBranch(path: String): String = {
    val commandLogger = GitCommandLogger()
    var branch = ""
    try {
      branch = Process(Seq("git", "rev-parse", "--short", "--abbrev-ref", "HEAD"), new File(path))
        .!!(commandLogger.gitCommandLogger).trim
    } catch {
      case e: Exception =>
        logging.error(s"fail to get branch of $path", e)
    }
    branch
  }

  def getBranches(path: String): Branches = {
    val commandLogger = GitCommandLogger()
    var head: String = ""
    var master: String = ""
    val branches = ListBuffer[String]()
    try {
      head = Process(Seq("git", "rev-parse", "--short", "--abbrev-ref", "HEAD"), new File(path))
        .!!(commandLogger.gitCommandLogger).trim
      master = Process(Seq("git", "rev-parse", "--abbrev-ref", "master"), new File(path))
        .!!(commandLogger.gitCommandLogger).trim
      Process(Seq("git", "branch", "--format=%(refname:short)"), new File(path))
        .lineStream(commandLogger.gitCommandLogger).foreach {
        string => branches.append(string.trim)
      }
    } catch {
      case e: Exception =>
        logging.error(s"fail to list branches of $path", e)
    }
    Branches(head = head, master = master, branch = branches.toList)
  }


  def switchBranch(path: String, branch: String): Boolean = {
    val commandLogger = GitCommandLogger()
    var done = false
    try {
      val exitCode = Process(Seq("git", "checkout", branch), new File(path)).!(commandLogger.gitCommandLogger)
      if (exitCode == 0) done = true
    } catch {
      case e: Exception =>
        logging.error(s"fail to checkout $branch", e)
    }
    done
  }

  def createBranch(path: String, branch: String): Boolean = {
    val commandLogger = GitCommandLogger()
    var done = false
    try {
      val isExists = Process(Seq("git", "branch", "--list", branch), new File(path))
        .!!(commandLogger.gitCommandLogger) //check exists
      if (isExists.isEmpty) {
        val exitCode = Process(Seq("git", "branch", branch), new File(path))
          .!(commandLogger.gitCommandLogger)
        if (exitCode == 0) done = true
      }
    } catch {
      case e: Exception =>
        logging.error(s"fail to create Branch $branch", e)
    }
    done
  }

  def deleteBranch(path: String, branch: String): Boolean = {
    val commandLogger = GitCommandLogger()
    var done = false
    try {
      val isExists = Process(Seq("git", "branch", "--list", branch), new File(path))
        .!!(commandLogger.gitCommandLogger) //check branch exists
      if (isExists.nonEmpty) {
        val exitCode = Process(Seq("git", "branch", "-D", branch), new File(path))
          .!(commandLogger.gitCommandLogger)
        if (exitCode == 0) done = true
      }
    } catch {
      case e: Exception =>
        logging.error(s"fail to delete Branch $branch", e)
    }
    done
  }

  def gitURL2Workspace(gitURL: String, localDir: String): String = {
    val suffix = gitURL.trim.split("/").takeRight(2)
    val userName = suffix(0)
    val projectName = suffix(1).substring(0, suffix(1).lastIndexOf("."))
    val stringBuffer = new StringBuffer(localDir)
      .append("/").append(userName)
      .append("/").append(projectName)
    stringBuffer.toString
  }

  case class GitCommandLogger() {
    val outputResult: ListBuffer[String] = ListBuffer[String]()
    val errorResult: ListBuffer[String] = ListBuffer[String]()
    val gitCommandLogger: ProcessLogger = ProcessLogger(line => outputResult.append(line),
      line => errorResult.append(line))
  }

}
