package com.pakius.common

import java.util.Date

/**
  * Created by FBecer01 on 21/10/2016.
  */
object Common {

  def parseDateGivenString(str : String) : Date = {
    val format = new java.text.SimpleDateFormat("dd-MM-yyyy")
    format.parse(str)
  }
}
