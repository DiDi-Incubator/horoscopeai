package com.didichuxing.horoscope.dsl

import org.antlr.v4.runtime.{BaseErrorListener, RecognitionException, Recognizer}

object SyntaxErrorListener extends BaseErrorListener {
  override def syntaxError(
    recognizer: Recognizer[_, _],
    offendingSymbol: Any, line: Int, charPositionInLine: Int,
    msg: String, e: RecognitionException): Unit = {
    throw new SyntaxException(
      s"line $line:$charPositionInLine $msg", e
    )
  }
}

class SyntaxException(message: String, cause: RecognitionException) extends Exception(message, cause)

class SemanticException(message: String) extends Exception(message)
