/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo

import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config
import com.xiaoju.apollo.sdk.Apollo

class ApolloService(config: Config) extends Logging {

  def start(): Unit = {
    Apollo.autoInit()
    info("Apollo service started")
  }

  def stop(): Unit = {
    Apollo.close()
    info("Apollo service stopped")
  }

}
