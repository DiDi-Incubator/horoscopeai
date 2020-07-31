/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.core

import com.didichuxing.horoscope.service.cluster.ClusterServiceBuilder
import com.didichuxing.horoscope.service.local.LocalServiceBuilder

object Horoscope {
  def newLocalService(): LocalServiceBuilder = new LocalServiceBuilder()

  def newClusterService(): ClusterServiceBuilder = new ClusterServiceBuilder()
}
