package Georgios.HW4

import util.control.Breaks._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import java.io._
import scala.collection.mutable.ListBuffer
import java.io.File
import java.lang.Math.{pow, sqrt}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.clustering.BisectingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import scala.annotation.tailrec
import scala.util.Random
import org.apache.spark.mllib.linalg.DenseVector


object Kmeans {
 
  
  def main(args: Array[String]) {
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      val conf = new SparkConf().setAppName("Kmeans-Georgios-Iliadis-INF553").setMaster("local[*]")
      val sc = new SparkContext(conf)
    
      /*
      val documents: RDD[Seq[String]] = sc.textFile(args(0))
          .map(_.split(" ").toSeq)
      val k = args(2).toInt
      val iterations = args(3).toInt
     	*/
      
      val documents: RDD[Seq[String]] = sc.textFile("yelp_reviews_clustering_small.txt").map(_.split(" ").toSeq)
      val k = 5
      val iterations = 20
      val seed = 20181031
      val feature = args(0).toString
      //val feature = "T" 
      //val feature = "W"
      
      if(feature == "W"){
       bigALGOw(seed,documents,sc,k,"W")
      }
      else if(feature == "T"){
       var reviewsInde = fixDataTF(documents)
       bigALGO(reviewsInde,seed,documents,sc,k, "T")
      }
      
      //var reviewsIndex = fixDataTF(documents)
      //val reviewsIndex = fixDataW(documents)
      
  }
     
     def EuclideanDist(veec1 : Vector, veec2: Vector): Double ={
          Math.sqrt(Vectors.sqdist(veec1, veec2))
     }
   
  def bigALGO(reviewsIndex :RDD[(Vector,Long)],seed :Int, documents: RDD[Seq[String]], sc: SparkContext, k:Int,feature : String) ={
      
//INITIALIZE CLUSTERS---------------------------------------------------------
      var centr1,centr2,centr3,centr4,centr5 = new ListBuffer[RDD[Double]]()
      var clust1,clust2,clust3,clust4,clust5 = new ListBuffer[Int]()
      
      def addtoCentroid(value: RDD[Double], number: Int){
         if(number == 0){centr1 += value}
         else if(number == 1){centr2 += value}
         else if(number == 2){centr3 += value}
         else if(number == 3){centr4 += value}
         else if(number == 4){centr5 += value}
      }
     
      def addToCluster(number: Int, value: Int) ={
         if(number == 0){clust1 += value}
         else if(number == 1){clust2 += value}
         else if(number == 2){clust3 += value}
         else if(number == 3){clust4 += value}
         else if(number == 4){clust5 += value}
      } 
      
      val r = new scala.util.Random(seed)
      for(i <- 0 to 4){
        val randomized = reviewsIndex.map(x=>x._1).map(x=>x(r.nextInt(reviewsIndex.collect().size)))
        addtoCentroid(randomized , i)
      }
     	val group_size = math.ceil(documents.collect.size/k.toDouble).toInt
      
      var SSEList: ListBuffer[Double] = ListBuffer(0,0,0,0,0,0,0,0)
      val centro1 = centr1.toList.map(x=>x.map(x=> Vectors.dense(x))).map(x=>x.collect().toList).flatten
      val centro2 = centr2.toList.map(x=>x.map(x=> Vectors.dense(x))).map(x=>x.collect().toList).flatten
      val centro3 = centr3.toList.map(x=>x.map(x=> Vectors.dense(x))).map(x=>x.collect().toList).flatten
      val centro4 = centr4.toList.map(x=>x.map(x=> Vectors.dense(x))).map(x=>x.collect().toList).flatten
      val centro5 = centr5.toList.map(x=>x.map(x=> Vectors.dense(x))).map(x=>x.collect().toList).flatten
      var allCentroids = List(centro1, centro2, centro3, centro4, centro5)
      var allClusters = List(clust1,clust2,clust3,clust4,clust5)
   
//DISTANCE OF POINTS TO CENTROIDS----------------------------------------------------
//ΠΕΝΤΕ ΦΟΡΕΣ ΤΟΥΤΟ!!
     
     for(lex <- 0 to 4){
        for(i <- 0 to 1000){
             println( "Value of i: " + i )
             var clusterChoice = 0
             val neoIndex = reviewsIndex.filter(x => x._2 == i)
             var mini = 1000000.0
             var distance = 0.0
             for (s <- 0 to 4){
                 val zd = allCentroids
                 val dis = neoIndex.map(x=> EuclideanDist(x._1 , allCentroids.flatten.apply(s)))
               									 .collect().toList.headOption.get
                 distance = distance + dis
                 SSEList(s) = distance
                 if(dis < mini){
               		  mini = dis
               		  clusterChoice = s
                 }
             }
//ASSIGN TO CLUSTER--------------------------------------------
             print("CLUSTER CHOICE: " + clusterChoice + " ! REVIEW TO ADD: " + i + "\n")
             addToCluster(clusterChoice,i)       
         }
        
//Update allClusters with new values of each cluster to calculate the centroids
       allClusters = List(clust1,clust2,clust3,clust4,clust5)
         
//UPDATE CENTROIDS -------------------------------------------------
       for( i <- 0 to 4){
         val siz = allClusters(i).size
         var me = 0.0
         for( j <- 0 to siz){
             me = me + allClusters(i)(j)
         }
         val clustCentroid = me / siz.toDouble
         val clustC: ListBuffer[Double] = ListBuffer[Double]()
         clustC += clustCentroid
         val add = sc.parallelize(clustC.toList)
         addtoCentroid(add,i)
       }
     
     }
    
        //print("SSE For Each CLUSTER:   " + "\n")
        //print(SSEList + "\n")
        
//FINALIZE RESULTS ---------------------------        
        val docuWithIndex = documents.zipWithIndex().map(x=> ((x._1.groupBy(identity).mapValues(_.size).toList) , x._2))
        var FinalResults: ListBuffer[List[String]] = ListBuffer()
        
        //print("ALL CLUSTERS: " + "\n")
        //print(allClusters)
        
        for(zwi <- 0 to 4){
          var fin: RDD[List[(String,Int)]] = sc.emptyRDD
          for(ze <- 0 to (allClusters(zwi).size-1) ){
            val wc = docuWithIndex.filter(x=> x._2 == allClusters(zwi)(ze)).map(x => x._1)
            fin = fin ++ wc
          }
          
          val boom = fin.collect().toList.flatten.sortBy(x => -x._2)
          val Top10F = sc.parallelize(boom).map(x=>x._1).collect().toList.take(10) 
          print("TOP 10 : " + Top10F + "\n")
          FinalResults += Top10F
        }
        /*
        print("FINAL RESULTS!" +"\n")
        print(FinalResults)
        print("ACCESSING THE RESULTS OF ALL THE CLUSTERS" +"\n")
        print(FinalResults(0) + "\n")
        print(FinalResults(1) + "\n")
        print(FinalResults(2) + "\n")
        print(FinalResults(3) + "\n")
        print(FinalResults(4) + "\n")
        print("SSE LIST NA DOUMEN POSO EN TO SSE GIA KATHE CLUSTER"+ "\n")
        print(SSEList.toList + "\n")
        */
        
        var TotCost = 0.0
        print(SSEList.toList.size)
        for(ella <- (0 to SSEList.toList.size -1)){
          TotCost = TotCost + SSEList(ella)
        }
        
//WRITE OUTPUT----------------------------------------- 
        val pw = new PrintWriter(new File("Georgios_Iliadis_Kmeans_small_T_5_20.json" ))
        pw.write("algorithm : Kmeans," + "\n" + "WSSE: " + TotCost + ",")
        pw.write("clusters:[ {" + "\n")
        for(fi <- 0 to 7){
            val numeron = fi+1
            pw.write("\t" + "id: " + numeron + "," + "\n")
            pw.write("\t" + "size: " + allClusters(fi).size + "," + "\n")
            pw.write("\t" + "error: " + SSEList(fi) + "," + "\n")
            pw.write("\t" + "terms: " + FinalResults(fi) + "," + "\n")
            pw.write("},")
        }
        pw.close    
  }
  
  def bigALGOw(seed :Int, documents: RDD[Seq[String]], sc: SparkContext, k:Int,feature : String) ={
      
     val docuWithIndex = documents.zipWithIndex().map(x=> ((x._1.groupBy(identity).mapValues(_.size).toList) , x._2))
     val vex = docuWithIndex.map(x=> ( (x._1.map(x=> Vectors.dense(x._2))) , x._2) )
     .map(x=>(x._1.toVector, x._2))
     var reviewsIndex = vex
     
//INITIALIZE CLUSTERS---------------------------------------------------------
      var centr1,centr2,centr3,centr4,centr5 = new ListBuffer[RDD[Double]]()
      var clust1,clust2,clust3,clust4,clust5 = new ListBuffer[Int]()
      
      def addtoCentroid(value: RDD[Double], number: Int){
         if(number == 0){centr1 += value}
         else if(number == 1){centr2 += value}
         else if(number == 2){centr3 += value}
         else if(number == 3){centr4 += value}
         else if(number == 4){centr5 += value}
      }
     
      def addToCluster(number: Int, value: Int) ={
         if(number == 0){clust1 += value}
         else if(number == 1){clust2 += value}
         else if(number == 2){clust3 += value}
         else if(number == 3){clust4 += value}
         else if(number == 4){clust5 += value}
      } 
      val r = new scala.util.Random(seed)
      for(i <- 0 to 4){
        val raa = reviewsIndex.map(x=>x._1.map(x=>x(r.nextInt(reviewsIndex.collect().size))))
        val sopa = raa.map(x=>x.toArray)
        val sopa2 = sc.parallelize(sopa.collect().flatten.toSeq)
        addtoCentroid(sopa2 , i)
      }
     	val group_size = math.ceil(documents.collect.size/k.toDouble).toInt
      
      var SSEList: ListBuffer[Double] = ListBuffer(0,0,0,0,0,0,0,0)
      val centro1 = centr1.toList.map(x=>x.map(x=> Vectors.dense(x))).map(x=>x.collect().toList).flatten
      val centro2 = centr2.toList.map(x=>x.map(x=> Vectors.dense(x))).map(x=>x.collect().toList).flatten
      val centro3 = centr3.toList.map(x=>x.map(x=> Vectors.dense(x))).map(x=>x.collect().toList).flatten
      val centro4 = centr4.toList.map(x=>x.map(x=> Vectors.dense(x))).map(x=>x.collect().toList).flatten
      val centro5 = centr5.toList.map(x=>x.map(x=> Vectors.dense(x))).map(x=>x.collect().toList).flatten
      var allCentroids = List(centro1, centro2, centro3, centro4, centro5)
      var allClusters = List(clust1,clust2,clust3,clust4,clust5)
   
//DISTANCE OF POINTS TO CENTROIDS----------------------------------------------------
//ΠΕΝΤΕ ΦΟΡΕΣ ΤΟΥΤΟ!!
     
     for(lex <- 0 to 4){
        for(i <- 0 to 1000){
             println( "Value of i: " + i )
             var clusterChoice = 0
             val neoIndex = reviewsIndex.filter(x => x._2 == i)
             var mini = 1000000.0
             var distance = 0.0
             for (s <- 0 to 4){
                 val zd = allCentroids
                 val dis = neoIndex.map(x=>x._1.map(y => EuclideanDist(y , allCentroids.flatten.apply(s))))
               									 .collect().toList.map(x=>x.toList).flatten.headOption.get
                 distance = distance + dis
                 SSEList(s) = distance
                 if(dis < mini){
               		  mini = dis
               		  clusterChoice = s
                 }
             }
//ASSIGN TO CLUSTER--------------------------------------------
             print("CLUSTER CHOICE: " + clusterChoice + " ! REVIEW TO ADD: " + i + "\n")
             addToCluster(clusterChoice,i)       
         }
        
//Update allClusters with new values of each cluster to calculate the centroids
       allClusters = List(clust1,clust2,clust3,clust4,clust5)
         
//UPDATE CENTROIDS -------------------------------------------------
       for( i <- 0 to 4){
         val siz = allClusters(i).size
         var me = 0.0
         for( j <- 0 to siz){
             me = me + allClusters(i)(j)
         }
         val clustCentroid = me / siz.toDouble
         val clustC: ListBuffer[Double] = ListBuffer[Double]()
         clustC += clustCentroid
         val add = sc.parallelize(clustC.toList)
         addtoCentroid(add,i)
       }
     
     }
    
        
//FINALIZE RESULTS ---------------------------        
        //val docuWithIndex = documents.zipWithIndex().map(x=> ((x._1.groupBy(identity).mapValues(_.size).toList) , x._2))
        var FinalResults: ListBuffer[List[String]] = ListBuffer()
        print("EDW ALL CLUSTERS: " + "\n")
        print(allClusters)
        
        for(zwi <- 0 to 4){
          print("YO" + "\n")
          var fin: RDD[List[(String,Int)]] = sc.emptyRDD
          for(ze <- 0 to (allClusters(zwi).size-1) ){
            val wc = docuWithIndex.filter(x=> x._2 == allClusters(zwi)(ze)).map(x => x._1)
            fin = fin ++ wc
          }
          
          val boom = fin.collect().toList.flatten.sortBy(x => -x._2)
          val Top10F = sc.parallelize(boom).map(x=>x._1).collect().toList.take(10) 
          print("TOP 10: " + Top10F + "\n")
          FinalResults += Top10F
        }
        
        /*
        print("SSE For Each CLUSTER:   " + "\n")
        print(SSEList + "\n")
        print("TA FINAL RESULTS!!!!!! " +"\n")
        print(FinalResults)
        print("ACCESSING THE RESULTS OF ALL THE CLUSTERS" +"\n")
        print(FinalResults(0) + "\n")
        print(FinalResults(1) + "\n")
        print(FinalResults(2) + "\n")
        print(FinalResults(3) + "\n")
        print(FinalResults(4) + "\n")
        print("SSE LIST: SSE FOR EACH CLUSTER"+ "\n")
        print(SSEList.toList + "\n")
        */
        
        var TotCost = 0.0
        print(SSEList.toList.size)
        for(ella <- (0 to SSEList.toList.size -1)){
          TotCost = TotCost + SSEList(ella)
        }
        
//WRITE OUTPUT-----------------------------------------
          val pw = new PrintWriter(new File("Georgios_Iliadis_Kmeans_small_W_5_20.json" ))
          pw.write("algorithm : Kmeans," + "\n" + "WSSE: " + TotCost + ",")
          pw.write("clusters:[ {" + "\n")
          for(fi <- 0 to 7){
            val numeron = fi+1
            pw.write("\t" + "id: " + numeron + "," + "\n")
            pw.write("\t" + "size: " + allClusters(fi).size + "," + "\n")
            pw.write("\t" + "error: " + SSEList(fi) + "," + "\n")
            pw.write("\t" + "terms: " + FinalResults(fi) + "," + "\n")
            pw.write("},")
          }
          pw.close   
  }
 
      def fixDataTF(doc : RDD[Seq[String]]): RDD[(Vector,Long)] = {
           val hashingTF = new HashingTF()
           val tf: RDD[Vector] = hashingTF.transform(doc)
           tf.cache()
           val idf = new IDF().fit(tf)
           val tfidf: RDD[Vector] = idf.transform(tf)
           val ReviewsIndex  = tfidf.zipWithIndex()
           return ReviewsIndex
      }
      
  
}
  
