/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo.convert

import com.didichuxing.horoscope.runtime._
import com.xiaoju.apollo.sdk.ApolloUser
import com.didichuxing.horoscope.runtime.convert._
import com.didichuxing.horoscope.apollo.{ApolloException, Constants}
import com.didichuxing.horoscope.util.Logging


trait ApolloUserConvertible {
  implicit val valueToApolloUser: Value.To[ApolloUser] = newTo {
    case valueDict: ValueDict =>
      ApolloUserConverter(valueDict)
    case v@_ =>
      throw new ApolloException(s"Unsupported value type ${v.toString}")
  }
}

object ApolloUserConverter extends Logging {
  def apply(valueDict: ValueDict): ApolloUser = {
    val apolloUser =
      valueDict.at(Constants.APOLLO_UID) match {
        case None =>
          throw new ApolloException(s"Can't find ${Constants.APOLLO_UID} field in ${valueDict.toString}")

        case Some(value: Text) =>
          new ApolloUser(value.underlying)

        case Some(v: Value) =>
          throw new ApolloException(s"Invalid value type ${v}, ${Constants.APOLLO_UID} must be in string type")
      }

    valueDict.foreach { case (key, value) =>
      value match {
        case Text(v) =>
          apolloUser.`with`(key, v)
        case NumberValue(n) =>
          apolloUser.`with`(key, n.toString())
        case BooleanValue(b) =>
          apolloUser.`with`(key, b.toString)
        case NULL => // do nothing
          logging.warn(s"Key ${key} is null")
        case v@(_: Binary | _: ValueList | _: ValueDict) =>
          apolloUser.`with`(key, v.toString())
        case _ =>
          throw new ApolloException(s"Unsupported value type for ApolloUser key ${key}")
      }
    }

    apolloUser
  }
}
