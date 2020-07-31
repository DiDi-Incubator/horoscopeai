/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.resource

import com.didichuxing.horoscope.util.{Constants, Logging, Utils}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SlotSuite extends FunSuite with BeforeAndAfter with Logging {

  test("trace slot") {
    //0,5461  5461,10922  10922,16384
    info(("slot", Utils.getSlot("b99604d5cf593976929a8131819ee941", Constants.CLUSTER_SLOT_COUNT)))//7163  2553
    info(("slot", Utils.getSlot("9e60108bf9c6314aaa42e4b78bec644f", Constants.CLUSTER_SLOT_COUNT)))//16298 2554
    info(("slot", Utils.getSlot("11eda50d530d371b9e9660e7beea3545", Constants.CLUSTER_SLOT_COUNT)))//806   2552
  }
}
