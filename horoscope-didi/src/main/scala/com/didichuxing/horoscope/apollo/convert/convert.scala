/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo

package object convert {

  // scalastyle:off
  object implicits extends FeatureToggleConvertible
    with ApolloConfigConvertible with ApolloUserConvertible with Serializable

}
