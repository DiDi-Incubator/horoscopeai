package com.didichuxing.horoscope.util

import java.security.SecureRandom
import java.util.concurrent.atomic.AtomicInteger
import java.nio.ByteBuffer

//https://docs.mongodb.com/manual/reference/method/ObjectId/#ObjectId
object ObjectID {
  private val OBJECT_ID_LENGTH = 12
  private val LOW_ORDER_THREE_BYTES = 0x00ffffff
  private val secureRandom = new SecureRandom
  // Use primitives to represent the 5-byte random value.
  private val RANDOM_VALUE1 = secureRandom.nextInt(0x01000000)
  private val RANDOM_VALUE2 = secureRandom.nextInt(0x00008000).toShort

  private val NEXT_COUNTER = new AtomicInteger(new SecureRandom().nextInt)

  private val HEX_CHARS = Array[Char]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

  private def int3(x: Int) = (x >> 24).toByte

  private def int2(x: Int) = (x >> 16).toByte

  private def int1(x: Int) = (x >> 8).toByte

  private def int0(x: Int) = x.toByte

  private def short1(x: Short) = (x >> 8).toByte

  private def short0(x: Short) = x.toByte

  def toByteArray: Array[Byte] = {
    val buffer = ByteBuffer.allocate(OBJECT_ID_LENGTH)
    putToByteBuffer(buffer)
    buffer.array // using .allocate ensures there is a backing array that can be returned
  }

  def putToByteBuffer(buffer: ByteBuffer): Unit = {
    val timestamp = (System.currentTimeMillis() / 1000).toInt
    val counter = NEXT_COUNTER.getAndIncrement() & LOW_ORDER_THREE_BYTES
    buffer.put(int3(timestamp))
    buffer.put(int2(timestamp))
    buffer.put(int1(timestamp))
    buffer.put(int0(timestamp))
    buffer.put(int2(RANDOM_VALUE1))
    buffer.put(int1(RANDOM_VALUE1))
    buffer.put(int0(RANDOM_VALUE1))
    buffer.put(short1(RANDOM_VALUE2))
    buffer.put(short0(RANDOM_VALUE2))
    buffer.put(int2(counter))
    buffer.put(int1(counter))
    buffer.put(int0(counter))
  }

  def next: String = {
    val chars = new Array[Char](OBJECT_ID_LENGTH * 2)
    var i = 0
    for (b <- toByteArray) {
      chars({
        i += 1;
        i - 1
      }) = HEX_CHARS(b >> 4 & 0xF)
      chars({
        i += 1;
        i - 1
      }) = HEX_CHARS(b & 0xF)
    }
    new String(chars)
  }
}
