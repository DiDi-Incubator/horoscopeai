package com.didichuxing.horoscope.examples.builtins

import com.didichuxing.horoscope.runtime.expression.DefaultBuiltIn.{defaultBuiltin, parseJson}
import com.didichuxing.horoscope.runtime.{Text, ValueDict}
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, DefaultBuiltIn, SimpleBuiltIn}

import java.io.File
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.collection.mutable.ListBuffer
import scala.io.Source

object ExampleBuiltIn {
  implicit val exampleBuiltin: BuiltIn = new SimpleBuiltIn.Builder()
      .addFunction("get_sentiment")(getSentiment _)
      .addFunction("get_current_path")(getCurrentPath _)
      .addFunction("get_files_from_path")(getFilesFromPath _)
      .addFunction("read_file_content")(readFileContent _)
      .build()

  def getSentiment(accessKeyId: String, accessKeySecret: String,
                   sortFiles: Array[String], root: String): Array[Array[ValueDict]] = {
    import com.aliyuncs.CommonRequest
    import com.aliyuncs.DefaultAcsClient
    import com.aliyuncs.profile.DefaultProfile
    val defaultProfile = DefaultProfile.getProfile(
      "cn-hangzhou",
      accessKeyId,
      accessKeySecret)
    val client = new DefaultAcsClient(defaultProfile)
    val res = new ListBuffer[Array[ValueDict]]
    for (file <- sortFiles ) {
      val contents = readFileContent(root, file)
      val tmp = new ListBuffer[ValueDict]
      for (content <- contents) {
        val text = parseJson(content).visit("text").toString()
        val text_replace = text.replace(" ", "")
        val t = text_replace.substring(1, text_replace.length - 1)
        if (!t.isEmpty()) {
          val request = new CommonRequest()
          request.setDomain("alinlp.cn-hangzhou.aliyuncs.com");
          request.setVersion("2020-06-29");
          request.setSysAction("GetSaChGeneral");
          request.putQueryParameter("ServiceCode", "alinlp")
          request.putQueryParameter("Text", t)
          val response = client.getCommonResponse(request)
          val valueDict = parseJson(parseJson(parseJson(response.getData())
            .visit("Data").asInstanceOf[Text].underlying)
            .visit("result").toString())
          tmp += valueDict
        }
      }
      res += tmp.toArray
    }
    res.toArray
  }

  def getCurrentPath(): String = {
    val file = new File("")
    file.getAbsolutePath()
  }

  def getFilesFromPath(dirPath: String): Array[String] = {
    new File(dirPath).list()

  }

  def readFileContent(root: String, file: String): Array[String] = {
    val source = Source.fromFile(root + "/" + file, "UTF-8")
    val lines = source.getLines().toArray
    source.close()
    lines
  }

  def hmacSHA1Encrypt(encryptText: String, encryptKey: String): Array[Byte] = {
    val data = encryptKey.getBytes("UTF-8")
    val secretKey = new SecretKeySpec(data, "HmacSHA1")
    val mac = Mac.getInstance("HmacSHA1")
    mac.init(secretKey)

    val text = encryptText.getBytes("UTF-8")
    mac.doFinal(text)
  }

}
