package com.pakius

import java.nio.file.Files
import java.sql.PreparedStatement

import com.pakius.helper.Common
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpec, GivenWhenThen, Matchers}


/**
  * Created by FBecer01 on 21/10/2016.
  */

class RDBInizializerTest extends FlatSpec with MockitoSugar with BeforeAndAfter with GivenWhenThen with Matchers {

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
    val res  =Common.splitLine(sc.parallelize(lines)).first()
    Then("line is")
    res should equal(Array(
      "user_000001", "m", "", "Japan","Aug 13, 2006"))
  }

  "split line" should "split and clean line" in {
    Given("a line")
    val lines = Array("#id\tgender\tage\tcountry\tregistered","user_000001\t\t\t")
    When("line is split")
    val res  =Common.splitLine(sc.parallelize(lines)).first()
    Then("line is")
    res should equal(Array(
      "user_000001"))
  }

  "map values" should "maps all value" in {
    Given("aray of values")
    val values = Array("user_000001", "m", "1", "Japan", "Aug 13, 2006")
    When("mapping")
    val ps = mock[PreparedStatement]
    RDBInitializer.mapValuesAndExecute(values, ps)
    Then("virify ")
    Mockito.verify(ps, Mockito.times(1)).setString(1,"user_000001")
    Mockito.verify(ps, Mockito.times(1)).setString(2,"m")
    Mockito.verify(ps, Mockito.times(1)).setInt(3,1)
    Mockito.verify(ps, Mockito.times(1)).setString(4,"Japan")


  }

  "map values only user id" should "maps only user id" in {
    Given("aray of values")
    val values = Array("user_000001")
    When("mapping")
    val ps = mock[PreparedStatement]
    RDBInitializer.mapValuesAndExecute(values, ps)
    Then("virify ")
    Mockito.verify(ps, Mockito.times(1)).setString(1,"user_000001")
    Mockito.verify(ps, Mockito.times(0)).setString(2,"")



  }





}
