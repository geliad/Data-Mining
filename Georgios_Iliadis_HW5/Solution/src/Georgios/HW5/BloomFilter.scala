package Georgios.HW5

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random
import scala.collection.mutable.Map
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import twitter4j.Status
import scala.collection.BitSet



object BloomFilter {
  
  //ΤΙ ΑΚΡΙΒΩΣ ΝΑ ΚΑΜΩ. ΕΧΩ το μαπ με τα hashtags.
  // TWRA PIANNW NEA TWEETS, ti kamnw akrivws??
  
  var bitArray = ListBuffer[Int]()
  var bitArrayStart = ListBuffer[Int]()
  var Tweet = new ListBuffer[Status]()
  var TweetLength = 0
  var hashtagsMapNew = Map[String, Int]()
  var hashtagsMapStart = Map[String,Int]()
  var FPositive = Map[String, String]()
  var FPositiveSure = Map[String, String]()
  
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
  
  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter() = {
    import scala.io.Source
    
    for (line <- Source.fromFile("../twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("BloomFilter")
    val sc = new SparkContext(conf)
    sc.setLogLevel(logLevel = "OFF")
    val ssc = new StreamingContext(sc, Seconds(10))
    setupTwitter()
    setupLogging()
    
    for(i <- 0 to 99){
      bitArrayStart += 0
      bitArray += 0
    }
    
    val tweets = TwitterUtils.createStream(ssc, None)
    tweets.foreachRDD(x => processor(x))
    ssc.start()
    ssc.awaitTermination()
  }
  
  /*
  def hashCode(thiz: String): Int = {
    var res = 0
    var mul = 1 // holds pow(31, length-i-1)
    var i = thiz.length-1
    while (i >= 0) {
      res += thiz.charAt(i) * mul
      mul *= 31
      i -= 1
    }
    res
  }
  */
  
  def h1(s: Int): Int={
    return Math.abs(s % 100)
  }
  
  def h2(s: Int): Int={
    return Math.abs(s % 50) + Math.abs(s % 51)
  }
  
  def h3(s: Int): Int={
    return Math.abs(s % 25) + Math.abs(s % 76)
  }
   
  //meta ta kainouria pou erkounte chekare to hashnumber tous an yparxei hdh -> maybe
  //                                                          an den yparxei -> NO!
  
  //chekare to kanonikon to hashmap an exei ekeines tes lexeis kai compare me tin nea lexi
  def processor(rddStatus : RDD[Status]) : Unit = {
    val expectedElements = 1000000
    val falsePositiveRate = 0.1
    val k = Math.ceil((bitArray.size / expectedElements) * Math.log(2.0)).toInt
    val expectedFalsePositiveProbability = Math.pow(1 - Math.exp(-k * 1.0 * expectedElements / bitArray.size), k)
    
    val tweet = rddStatus.collect()
    for(status <- tweet){
        TweetLength = TweetLength + status.getText().length
        Tweet.append(status)
        
        //Get hashtags
        //val hashtags = status.split(" ").filter(status => status.startsWith("#")).map(_.getText)
        val hashTags1 = status.getHashtagEntities().map(x => x.getText)
        
        //Add to map
        for(tag <- hashTags1){
          if(hashtagsMapNew.contains(tag)){
            hashtagsMapNew(tag) += 1
          }
          else{
            hashtagsMapNew(tag) = 1
          }
        }
       
        //NOMIZOUMEN eixa hashtagsMapStart
        for((k,v) <- hashtagsMapNew){
          val cod = k.hashCode
          val b1 = h1(cod)
          val b2 = h2(cod)
          val b3 = h3(cod)
          
          for (i <- bitArrayStart){
             if(bitArrayStart(b1) == 1 & bitArrayStart(b2) == 1 & bitArrayStart(b3) == 1){
                FPositive(k) = "M" 
             }
             else{
               //print("k doesnot appear! ")
               FPositive(k) = "N"
             }
          }  
          
          //add values to bitvector
          if (bitArray(b1) == 0){
            bitArray(b1) = 1 //insert 1 at value bit1
          }
          if (bitArray(b2) == 0){
            bitArray(b2) = 1
          }
          if (bitArray(b3) == 0){
            bitArray(b3) = 1
          }
        } 
           
        //We are sure
        for((k,v) <- hashtagsMapNew){
            if(hashtagsMapStart.contains(k)){
              FPositiveSure(k) = "Y"       
            }
            else{
              FPositiveSure(k) = "N"
              //print("--------------------DEN YPARXEI---------------" + "\n")
            }
        }
           
        var truth = 0
        var count = 0
        var fail = 0
        var correctN = 0
      
        //(k,v) <- hashtagsMapNew
        for(k <- hashTags1){
          if(FPositive(k) == "M"){
            if (FPositiveSure(k) == "Y"){
              truth = truth + 1
            }
            else {
              fail = fail + 1
            }
          }
          
          if(FPositive(k) == "N"){
            if(FPositiveSure(k) == "N"){
              correctN = correctN + 1
            } 
          }
          count = count + 1
        }
        
      //print("FPOSITIVE: " + FPositive + "\n")
      //print("FPOSSUREE: " + FPositiveSure +"\n")
      
      //count = the number of new tweets/hashtags that were received.
      if(count > 0){
        print("Bloom Filter estimated correctly that tweet might be present: " + truth + "\n")
        print("Bloom FIlter estimated correctly that tweet won't be present: " + correctN + "\n")
        print("Bloom Filter estimated incorrectly that tweet might be present: "+ fail + "\n")
        print("Count: " + count + "\n" + "\n" + "\n")
      }
      //  print("FRACTION: " + truth/count * 100 + "\n")
      
      
      bitArrayStart = bitArray
      //hashtagsMapStart = hashtagsMapNew
      for(tag <- hashTags1){
          if(hashtagsMapStart.contains(tag)){
            hashtagsMapStart(tag) += 1
          }
          else{
            hashtagsMapStart(tag) = 1
          }
      }
 
    }
  }
  
}