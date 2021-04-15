/*
 * Copyright (C) 2021 DiDi Inc. All Rights Reserved.
 * Authors: liuhangfan_i@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.util

import com.didichuxing.horoscope.service.storage.Branches
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.MergeResult._
import org.eclipse.jgit.api.errors._
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.transport.{PushResult, RefSpec, UsernamePasswordCredentialsProvider}

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.sys.process.{Process, ProcessLogger}

object GitUtil extends Logging {
  def importRepo(gitURL: String, repoDir: String, name: String, pw: String): Option[Any] = {
    try {
      require(gitURL.endsWith(".git"))
      val dir = Files.createDirectories(Paths.get(repoDir))
      if (dir.toFile.listFiles().isEmpty) {
        Git.cloneRepository.setURI(gitURL).setDirectory(dir.toFile)
          .setCredentialsProvider(new UsernamePasswordCredentialsProvider(name, pw))
          .call
      }
      Some(true)
    } catch {
      case e: IllegalArgumentException =>
        logging.error("fail to import", e)
        Some("invalid gitURL")
      case e: TransportException => //身份认证异常
        logging.error(s"fail to import", e)
        Some(e.toString)
      case e: InvalidRemoteException => //远程链接异常
        logging.error(s"fail to import", e)
        Some(e.toString)
      case e: Exception =>
        throw e
    }
  }

  //设置upstream仓库
  def setUpstream(path: String, gitURL: String): Option[Any] = {
    val commandLogger = GitCommandLogger()
    try {
      val exitCode = Process(Seq("git", "remote", "add", "upstream", gitURL), new File(path))
        .!(commandLogger.gitCommandLogger)
      if (exitCode == 0) {
        Some(true)
      } else {
        Some(commandLogger.errorResult.toList.mkString("", "\n", ""))
      }
    } catch {
      case e: Exception =>
        logging.error(s"fail to set upstream in $path", e)
        Some(e.toString)
    }
  }

  def updateRepo(path: String, remoteBranch: String, remote: String, name: String, pw: String): String = {
    val message = new StringBuffer()
    try {
      val repo = new FileRepository(path)
      val git: Git = new Git(repo)
      val pull = git.pull()
        .setRemote(remote)
        .setRemoteBranchName(remoteBranch)
        .setCredentialsProvider(new UsernamePasswordCredentialsProvider(name, pw))
      val pullResult = pull.call()
      if (pullResult.getMergeResult != null) {
        if (pullResult.getMergeResult.getMergeStatus.equals(MergeStatus.CONFLICTING)) {
          val conflict = pullResult.getMergeResult.getConflicts.asScala.keySet
          message.append("[CONFLICT]: \n")
          conflict.foreach(f => message.append(s"$f \n"))
        }
        pullResult.getMergeResult
        message.append(s"${pullResult.getMergeResult.toString} \n")
      }
    } catch {
      case e: TransportException =>
        logging.error(s"fail to update in $path", e)
        message.append(e.toString)
      case e: CheckoutConflictException =>
        logging.error(s"fail to update in $path", e)
        message.append("[CONFLICT]: \n")
        message.append(e.getConflictingPaths.asScala.toList.mkString("", "\n", ""))
      case e: GitAPIException =>
        logging.error(s"fail to update in $path", e)
        message.append(e.toString)
      case e: Exception =>
        logging.error(s"fail to update in $path", e)
        message.append(e.toString)
    }
    message.toString
  }

  def pushRepo(message: String, name: String, pw: String, path: String, branch: String,
               remote: String, refSpec: String): String = {
    val res = new StringBuffer()
    try {
      val repo = new FileRepository(path)
      require(branch.equals(repo.getBranch))
      val git: Git = new Git(repo)
      git.add.addFilepattern(".").call
      val commitRes: RevCommit = git.commit.setMessage(message).call
      res.append(s"[$branch ${commitRes.name().substring(0, 8)}] ")
      res.append(s"${commitRes.getShortMessage} \n")
      res.append(s"Committer: ${commitRes.getCommitterIdent.getName} \n")
      val push = git.push
        .setRemote(remote)
        .setForce(true)
        .setCredentialsProvider(new UsernamePasswordCredentialsProvider(name, pw))
        .setRefSpecs(new RefSpec(refSpec))

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
      case e: IllegalArgumentException =>
        s"fail to push, your branch has changed"
      case e: InvalidRemoteException =>
        "called with an invalid remote uri"
      case e: TransportException =>
        e.toString
      case e: GitAPIException =>
        e.toString
      case e: Exception =>
        logging.error(s"fail to push $path", e.printStackTrace())
        s"fail to push"
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
        e.toString
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
        e.toString
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
        e.toString
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


  def switchBranch(path: String, branch: String): Option[Any] = {
    val commandLogger = GitCommandLogger()
    try {
      val exitCode = Process(Seq("git", "checkout", branch), new File(path)).!(commandLogger.gitCommandLogger)
      if (exitCode == 0) {
        Some(true)
      } else {
        Some(commandLogger.errorResult.toList.mkString("", "\n", ""))
      }
    } catch {
      case e: Exception =>
        logging.error(s"fail to checkout $branch", e.toString)
        Some(e.toString)
    }
  }

  def createBranch(path: String, branch: String): Option[Any] = {
    val commandLogger = GitCommandLogger()
    try {
      val isExists = Process(Seq("git", "branch", "--list", branch), new File(path))
        .!!(commandLogger.gitCommandLogger) //check exists
      if (isExists.isEmpty) {
        val exitCode = Process(Seq("git", "branch", branch), new File(path))
          .!(commandLogger.gitCommandLogger)
        if (exitCode == 0) {
          Some(true)
        } else {
          Some(commandLogger.errorResult.toList.mkString("", "\n", ""))
        }
      } else {
        Some(commandLogger.errorResult.toList.mkString("", "\n", ""))
      }
    } catch {
      case e: Exception =>
        logging.error(s"fail to create Branch $branch", e)
        Some(e.toString)
    }
  }

  def deleteBranch(path: String, branch: String): Option[Any] = {
    val commandLogger = GitCommandLogger()
    try {
      val isExists = Process(Seq("git", "branch", "--list", branch), new File(path))
        .!!(commandLogger.gitCommandLogger) //check branch exists
      if (isExists.nonEmpty) {
        val exitCode = Process(Seq("git", "branch", "-D", branch), new File(path))
          .!(commandLogger.gitCommandLogger)
        if (exitCode == 0) {
          Some(true)
        } else {
          Some(commandLogger.errorResult.toList.mkString("", "\n", ""))
        }
      } else {
        Some(commandLogger.errorResult.toList.mkString("", "\n", ""))
      }
    } catch {
      case e: Exception =>
        logging.error(s"fail to delete Branch $branch", e)
        Some(e.toString)
    }
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
