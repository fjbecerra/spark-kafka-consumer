package com.pakius

import com.pakius.helper.Common
import org.apache.avro.generic.{GenericRecord, IndexedRecord}

/**
  * Created by FBecer01 on 11/11/2016.
  */

trait Music
case class Session(userName: String, lenghtSession:Long, startSession:Long)

object AvroConverter{

  def Event(line : Array[String]) :Event = {
    new Event(line(0), Common.parseDateGivenString(line(1), "yyyy-MM-dd'T'HH:mm:ss").getTime, line(2), line(3), line(4))
  }
}
