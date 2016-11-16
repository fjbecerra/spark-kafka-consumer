package com.pakius

import org.apache.avro.generic.{GenericRecord, IndexedRecord}

/**
  * Created by FBecer01 on 11/11/2016.
  */
object AvroConverter{

  def Event(line : Array[String]) :Event = {
    //Todo convert date to long
    new Event(line(0), line(1).toLong, line(2), line(3), line(4))
  }
}
