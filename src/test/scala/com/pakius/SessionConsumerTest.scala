package com.pakius

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by FBecer01 on 16/11/2016.
  */
@RunWith(classOf[JUnitRunner])
class SessionConsumerTest extends FunSuite{
//Todo fix tests
  test("find session") {
    val v1 = new Event("11", 35001l, "ss", "", "")
    val v2 = new Event("11", 35002l, "ss", "", "")
    val v3 = new Event("11", 35003l, "ss", "", "")
    val events = List(v1, v2, v3)
    SessionConsumer.pack(events)
  }

}
