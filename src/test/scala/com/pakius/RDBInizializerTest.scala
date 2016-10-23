package com.pakius

import java.nio.file.Files

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.{Matchers, GivenWhenThen, BeforeAndAfter, FlatSpec}


/**
  * Created by FBecer01 on 21/10/2016.
  */

class RDBInizializerTest extends FlatSpec with BeforeAndAfter with GivenWhenThen with Matchers {

  private val master = "local[2]"
  private val appName = "example-spark-streaming"
  private val batchDuration = Seconds(1)
  private val checkpointDir = Files.createTempDirectory(appName).toString

  private var sc: SparkContext = _
  private var ssc: StreamingContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDir)

    sc = ssc.sparkContext
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
  }


  "split line" should "split the line" in {
    Given("a line")
    val lines = Array("#id\tgender\tage\tcountry\tregistered","user_000001\tm\t\tJapan\tAug 13, 2006")
    When("line is split")
    val res  =RDBInitializer.splitLine(sc.parallelize(lines)).first()
    Then("line is")
    res should equal(Array(
      "user_000001", "m", "", "Japan","Aug 13, 2006"))
  }
  
  "split line" should "split and clean line" in {

  }





}
