/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: chaiyi@didiglobal.com
 */

package com.didichuxing.horoscope.util

import java.util.zip.{ZipEntry, ZipInputStream}
import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import com.google.protobuf.Descriptors.{Descriptor, FileDescriptor}
import com.google.protobuf.{DynamicMessage, Message}
import scala.collection.JavaConverters._
import scala.collection.mutable


object PBParserUtil extends Logging {
  private val messageTypeMap: Map[String, Descriptor] = loadDescriptors(getDescriptorFilesFromJar())

  def containsMessageType(className: String): Boolean = {
    messageTypeMap.contains(className)
  }

  def parseMessage(className: String, data: Array[Byte]): Message = {
    val startTime = System.nanoTime()
    val descriptor = messageTypeMap(className)
    val result = DynamicMessage.parseFrom(descriptor, data)
    val endTime = System.nanoTime()
    debug(s"Parse pb message of ${className} time taken ${endTime - startTime} ns")
    result
  }

  def loadDescriptors(descriptorFiles: Array[String]): Map[String, Descriptor] = {
    val descriptorMap = mutable.HashMap[String, FileDescriptor]()
    val messageMap = mutable.HashMap[String, Descriptor]()
    descriptorFiles.foreach { f =>
      info(s"load descriptor file, ${f}")
      val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream(f)
      val descriptorSet = FileDescriptorSet.parseFrom(stream)
      for (fdp <- descriptorSet.getFileList.asScala) {
        val dependencyList = fdp.getDependencyList
        val dependency = descriptorMap.filter { p => dependencyList.contains(p._1) }.values.toArray
        val fd = FileDescriptor.buildFrom(fdp, dependency)
        for (descriptor <- fd.getMessageTypes.asScala.toArray) {
          val packageName = if (fdp.getOptions.getJavaPackage.nonEmpty) {
            fdp.getOptions.getJavaPackage
          } else {
            fdp.getPackage
          }
          val className = packageName + "." + fdp.getOptions.getJavaOuterClassname + "." + descriptor.getName
          messageMap.put(className, descriptor)
        }
        descriptorMap.put(fdp.getName, fd)
      }
      stream.close()
    }

    info(s"load descriptor file finished, got ${messageMap.size} message")
    messageMap.toMap
  }

  private def getDescriptorFilesFromJar(): Array[String] = {
    val files = new mutable.ArrayBuffer[String]()
    try {
      val codeSource = this.getClass.getProtectionDomain.getCodeSource
      if (codeSource != null) {
        val jarURL = codeSource.getLocation
        info(s"jar url, ${jarURL.toString}")
        val zipStream = new ZipInputStream(jarURL.openStream())
        var ze = new ZipEntry("")
        while (ze != null) {
          ze = zipStream.getNextEntry
          if (ze != null) {
            val entryName = ze.getName
            if (entryName.startsWith("protobuf/descriptor-sets") && entryName.endsWith(".protobin")) {
              files.append(entryName)
            }
          }
        }
        zipStream.close()
      }
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        error("Got an exception when extract pb descriptor file names from jar")
    }
    files.toArray
  }
}
