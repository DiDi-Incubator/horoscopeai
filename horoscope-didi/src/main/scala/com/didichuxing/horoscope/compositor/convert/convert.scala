/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.compositor

import com.didichuxing.horoscope.apollo.convert.ApolloConfigConvertible

package object convert {
  object Implicits extends ApolloConfigConvertible with TypeSafeConfigConvertible with Serializable
}
