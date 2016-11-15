package com.pakius

import org.apache.avro.generic.{GenericRecord, IndexedRecord}

/**
  * Created by FBecer01 on 11/11/2016.
  */
object AvroConverter{

  def Event(line : Array[String]) = {
    new Event(line(0), line(1), line(2), line(3), line(4))
  }
}
