/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.resource

class Participant(host: String, port: Int) extends java.io.Serializable {

  def getParticipantId(): String = {
    s"${host}:${port}"
  }

  def getHost(): String = {
    host
  }

  def getPort(): Int = {
    port
  }

  override def toString: String = {
    s"${host}:${port}"
  }

  override def hashCode(): Int = {
    getParticipantId().hashCode
  }

  override def equals(obj: Any): Boolean = {
    this.getParticipantId() == obj.asInstanceOf[Participant].getParticipantId()
  }
}
