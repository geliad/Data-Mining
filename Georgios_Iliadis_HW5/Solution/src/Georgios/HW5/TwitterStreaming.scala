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


/*
 * TwitterStreamingAppGeorgios\
 * 
 * In task 1, you need to have a list(Reservoir) has the capacity limit of 100, which can only save 100 tweets. 
 * When the streaming of the tweets coming, for the ﬁrst 100 tweets, we can directly save them in the list. 
 * After that, for the nth tweet, with probability 100/n , keep the nth tweet, else discard it. 
 * If you will keep the nth tweets, it will replace one of the tweet in list, you need to randomly pick one to be replaced
 *
 * 
 * Created a text file and loaded all the necessary staff instead of entering the in the input
 * since there are lots of characters, and it is too easy to make a mistake.
 * I created such a text file for simpicity in order to run the program easier.
 */

object TwitterStreaming {
  
  var hashtagsMap = Map[String, Int]()
  var Tweet = new ListBuffer[Status]()
  var TweetLength = 0
  var N = 0
  
 //makes sure only error messages get logged to avoid log spam
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
  
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

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val ssc = new StreamingContext("local[*]", "TwitterStreaming", Seconds(10))
    
    //Configure Twitter credentials using twitter.txt
    setupTwitter()
    setupLogging()
    
    val tweets = TwitterUtils.createStream(ssc, None)
    tweets.foreachRDD(x => processor(x))
    
    ssc.start()
    ssc.awaitTermination()
    
  }  
  
  def processor(rddStatus : RDD[Status]) : Unit = {
   
    val tweet = rddStatus.collect()
    for(status <- tweet){
      if(N < 100){
        TweetLength = TweetLength + status.getText().length
        Tweet.append(status)
        
        //Get hashtags
        //val hashtags = status.split(" ").filter(status => status.startsWith("#")).map(_.getText)
        val hashtags = status.getHashtagEntities().map(x => x.getText)
        //val hashtags1 = status.getHashtagEntities().map(_.getText)
        
        //Add to map
        for(tag <- hashtags){
          if(hashtagsMap.contains(tag)){
            hashtagsMap(tag) += 1
          }
          else{
            hashtagsMap(tag) = 1
          }
        }
      }

      
      //random arithmos kai loop
      else{
        //dame inta fasi paize epeidh meta en tha mporei -> N, S=100, (n/s)
        val rand = Random.nextInt(N)
        if(rand < 100){
          
          val removeThis = Tweet(rand)
          Tweet(rand) = status
          TweetLength = TweetLength + status.getText().length - removeThis.getText().length

          //Remove old
          val hashtags = removeThis.getHashtagEntities().map(_.getText)
          for(tag <- hashtags){
            hashtagsMap(tag) -= 1
          }

          //Add the new to hashtagmap
          //val newHashtags = status.split(" ").filter(status => status.startsWith("#")).map(_.getText)
          val newHashtags = status.getHashtagEntities().map(_.getText)
          for(tag <- newHashtags){
            if(hashtagsMap.contains(tag)){
              hashtagsMap(tag) += 1 //increment if there
            }
            else{
              hashtagsMap(tag) = 1
            }
          }

          
          //Output
          print("The number of the twitter from beginning: " + (N + 1) + "\n")
          print("Top 5 hot hashtags:" + "\n")
          
          //sort 
          val hotHashtag = hashtagsMap.toSeq
          //val sorted = hotHashtag.map(x=>x._2).sortWith(x > x)
          val sorted = hotHashtag.sortWith(_._2 > _._2)
          
          val top5 = sorted.size.min(5)
          for(i <- 0 until top5){
            if(sorted(i)._2 != 0){
              println(sorted(i)._1 + ":" + sorted(i)._2)
            }
          }
          
          val avg = TweetLength / 100.0
          println("The average length of the twitter is: " +  avg + "\n" + "\n")
        }
      }
      N = N + 1
    }
  }
}  
  
