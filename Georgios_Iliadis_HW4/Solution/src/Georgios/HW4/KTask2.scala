package Georgios.HW4

  
  import org.apache.spark._
  import org.apache.spark.SparkContext._
  import org.apache.log4j._
  import org.apache.spark.sql._
  import java.io._
  import org.apache.spark.mllib.feature.{HashingTF, IDF}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.SparkContext._
  import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
  import org.apache.spark.mllib.clustering.BisectingKMeans
  import org.apache.spark.mllib.linalg.{Vector, Vectors}
  import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
  import org.apache.spark.sql.functions.udf
  import scala.collection.mutable.ListBuffer
  import scala.collection.JavaConversions  

object KTask2 {
  
    
    
   def EuclideanDist(veec1 : Vector, veec2: Vector): Double ={
          Math.sqrt(Vectors.sqdist(veec1, veec2))
   }
   
   
   def main(args: Array[String]) {
     
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName("Kmeans-Georgios-Iliadis-INF553").setMaster("local[*]")
        val sc = new SparkContext(conf)

        /*val documents: RDD[Seq[String]] = sc.textFile(args(0)).map(_.split(" ").toSeq)
          val algo = args(1).toString
          val numClusters = args(2).toInt
          val numIterations = args(3).toInt          
     	*/
        val documents: RDD[Seq[String]] = sc.textFile("yelp_reviews_clustering_small.txt").map(_.split(" ").toSeq)
        documents.take(1).foreach(println)
      
        val hashingTF = new HashingTF()
        val tf: RDD[Vector] = hashingTF.transform(documents)
        tf.take(1).foreach(println)
      
        tf.cache()
        val idf = new IDF().fit(tf)
        val tfidf: RDD[Vector] = idf.transform(tf)
        tfidf.take(1).foreach(println)
        
        val numClusters = 8
        val numIterations = 20
        val seed = 42
        
        //val algo = "B"
        //val algo = "K"
        val algo = args(0).toString
        
        if(algo == "K"){
           kMeans(numClusters,numIterations ,tfidf, documents, seed, sc)
        }
        else if(algo == "B"){
           bisecting(numClusters,seed ,numIterations, tfidf ,documents,sc)
        }
        
      
 }
        
//----------------------------------------------KMEANS------------------------------------
def kMeans(numClusters : Int,numIterations: Int,tfidf: RDD[Vector],documents :RDD[Seq[String]], randomSeed : Int, sc: SparkContext)={
        
        val clusters = KMeans.train(tfidf, numClusters, numIterations,"random", randomSeed)
        val zz = clusters.predict(tfidf)
        val ReviewsIndex  = tfidf.zipWithIndex()  //.map(x => (1, x))
           
        var clus0 ,clus1,clus2,clus3,clus4,clus5,clus6,clus7= new ListBuffer[Int]()
     
        def addToCluster(number: Int, value: Int) ={
          if(number == 0){clus0 += value}
          else if(number == 1){clus1 += value}
          else if(number == 2){clus2 += value}
          else if(number == 3){clus3 += value}
          else if(number == 4){clus4 += value}
          else if(number == 5){clus5 += value}
          else if(number == 6){clus6 += value}
          else if(number == 7){clus7 += value}
        }
       
       var SSEList: ListBuffer[Double] = ListBuffer(0,0,0,0,0,0,0,0)
      
       for(i <- 0 to 999){
           println( "Value of i: " + i )
           var clusterChoice = 0
           val neoIndex = ReviewsIndex.filter(x => x._2 == i)
           var mini = 1000000.0
           var distance = 0.0
           for (s <- 0 to 7){
             val dis = neoIndex.map(x=> EuclideanDist(x._1 , clusters.clusterCenters.apply(s)))
             									 .collect().toList.headOption.get
             distance = distance + dis
             SSEList(s) = distance
             if(dis < mini){
             		mini = dis
             		clusterChoice = s
             }
           }
           print("CLUSTER CHOICE: " + clusterChoice + " ! REVIEW TO ADD: " + i + "\n")
           addToCluster(clusterChoice,i)
       }
       
        //print("SSEList:   " + "\n")
        //print(SSEList + "\n")
        
       	var clust1 = clus0.toList
       	var clust2 = clus1.toList
       	var clust3 = clus2.toList
       	var clust4 = clus3.toList
       	var clust5 = clus4.toList
       	var clust6 = clus5.toList
       	var clust7 = clus6.toList
       	var clust8 = clus7.toList
       	
        print("Look inside clusters: " + "\n")
        print(clust1 + "\n" + clust2 + "\n" + clust3 + "\n" + clust4 + "\n" + clust5 + "\n" + clust6 + "\n" + clust7 + "\n" + clust8 + "\n")
        
        
        val docuWithIndex = documents.zipWithIndex().map(x=> ((x._1.groupBy(identity).mapValues(_.size).toList) , x._2))
        var FinalResults: ListBuffer[List[String]] = ListBuffer()
        var allClusters = List(clust1,clust2,clust3,clust4,clust5,clust6,clust7,clust8)//......
        //print("EDW ALL CLUSTERS: " + "\n")
        //print(allClusters)
        
        
        for(zwi <- 0 to 7){
          print("YO" + "\n")
          var fin: RDD[List[(String,Int)]] = sc.emptyRDD
          for(ze <- 0 to (allClusters(zwi).size-1) ){
            val wc = docuWithIndex.filter(x=> x._2 == allClusters(zwi)(ze)).map(x => x._1)
            fin = fin ++ wc
          }
          val boom = fin.collect().toList.flatten.sortBy(x => -x._2)
          val Top10F = sc.parallelize(boom).map(x=>x._1).collect().toList.take(10) 
          print("TOP 10 KOKO: " + Top10F + "\n")
          FinalResults += Top10F
        }
        /*
        print("TA FINAL RESULTS!!!!!! " +"\n")
        print(FinalResults)
        print("ACCESSING THE RESULTS OF ALL THE CLUSTERS" +"\n")
        print(FinalResults(0) + "\n")
        print(FinalResults(1) + "\n")
        print(FinalResults(2) + "\n")
        print(FinalResults(3) + "\n")
        print(FinalResults(4) + "\n")
        print(FinalResults(5) + "\n")
        print(FinalResults(6) + "\n")
        print(FinalResults(7) + "\n")
        print("SSE LIST NA DOUMEN POSO EN TO SSE GIA KATHE CLUSTER"+ "\n")
        print(SSEList.toList + "\n")
       */
        var TotCost = 0.0
        print(SSEList.toList.size)
        for(ella <- (0 to SSEList.toList.size -1)){
          TotCost = TotCost + SSEList(ella)
        }
        //writeOutput(K, TotCost, SSEList, allClusters, FinalResults)     

        val pw = new PrintWriter(new File("Georgios_Iliadis_Kmeans_small_8_20.json" ))
        pw.write("algorithm : Kmeans," + "\n" + "WSSE: " + TotCost + ",")
          pw.write("clusters:[ " + "\n")
          for(fi <- 0 to 7){
            val numeron = fi+1
            pw.write("{" +"\n" + "\t" + "id: " + numeron + "," + "\n")
            pw.write("\t" + "size: " + allClusters(fi).size + "," + "\n")
            pw.write("\t" + "error: " + SSEList(fi) + "," + "\n")
            pw.write("\t" + "terms: " + FinalResults(fi) + "\n")
            pw.write("}," + "\n")
          }
          pw.write("]")
          pw.close  


        
 }
 
//----------------___________------------------BISECTING--______________------------------------------------
def bisecting(numClusters : Int, randomSeed: Int, numIterations: Int, tfidf: RDD[Vector], documents :RDD[Seq[String]], sc: SparkContext)={
        val bkm = new BisectingKMeans().setK(numClusters).setSeed(randomSeed).setMaxIterations(numIterations)
        val model = bkm.run(tfidf)
        val modello = model.predict(tfidf)
                
        val BReviewsIndex  = tfidf.zipWithIndex()  //.map(x => (1, x))
          
        var Bclus0 = new ListBuffer[Int]()
        var Bclus1 = new ListBuffer[Int]()
        var Bclus2 = new ListBuffer[Int]()
        var Bclus3 = new ListBuffer[Int]()
        var Bclus4 = new ListBuffer[Int]()
        var Bclus5 = new ListBuffer[Int]()
        var Bclus6 = new ListBuffer[Int]()
        var Bclus7 = new ListBuffer[Int]()

        def BaddToCluster(number: Int, value: Int) ={
          if(number == 0){Bclus0 += value}
          else if(number == 1){Bclus1 += value}
          else if(number == 2){Bclus2 += value}
          else if(number == 3){Bclus3 += value}
          else if(number == 4){Bclus4 += value}
          else if(number == 5){Bclus5 += value}
          else if(number == 6){Bclus6 += value}
          else if(number == 7){Bclus7 += value}
        }
       
       var BSSEList: ListBuffer[Double] = ListBuffer(0,0,0,0,0,0,0,0)
      
       for(i <- 0 to 5){
           println( "Value of a: " + i )
           var BclusterChoice = 0
           val BneoIndex = BReviewsIndex.filter(x => x._2 == i)
           var Bmini = 1000000.0
           var Bdistance = 0.0
           for (s <- 0 to 7){
             val Bdis = BneoIndex.map(x=> EuclideanDist(x._1 , model.clusterCenters.apply(s)))
             									 .collect().toList.headOption.get
             Bdistance = Bdistance + Bdis
             BSSEList(s) = Bdistance
             if(Bdis < Bmini){
             		Bmini = Bdis
             		BclusterChoice = s
             }
           }
           print("EDW THA DOUMEN TO CLUSTER CHOICE: " + BclusterChoice + " ! KAI poion REVIEW NA KAMEI ADD: " + i + "\n")
           BaddToCluster(BclusterChoice,i)
        }
       
        print("EDW THA DOUMEN POSON EINAI TO SSE GIA KATHE CLUSTER:   " + "\n")
        print(BSSEList + "\n")
        
       	var Bclust1 = Bclus0.toList
       	var Bclust2 = Bclus1.toList
       	var Bclust3 = Bclus2.toList
       	var Bclust4 = Bclus3.toList
       	var Bclust5 = Bclus4.toList
       	var Bclust6 = Bclus5.toList
       	var Bclust7 = Bclus6.toList
       	var Bclust8 = Bclus7.toList
       	
        print("DE DAMEEEEE TA CLUSTERS ME PIA VALUES EXOUN MESAAA: " + "\n")
        print(Bclust1 + "\n" + Bclust2 + "\n" + Bclust3 + "\n" + Bclust4 + "\n" + Bclust5 + "\n" + Bclust6 + "\n" + Bclust7 + "\n" + Bclust8 + "\n")
        
    
        val BdocuWithIndex = documents.zipWithIndex().map(x=> ((x._1.groupBy(identity).mapValues(_.size).toList) , x._2))
        var BFinalResults: ListBuffer[List[String]] = ListBuffer()
        var BallClusters = List(Bclust1,Bclust2,Bclust3,Bclust4,Bclust5,Bclust6,Bclust7,Bclust8)//......
        //print("EDW ALL CLUSTERS: " + "\n")
        //print(BallClusters)
        
        
        for(Bzwi <- 0 to 7){
          print("YO" + "\n")
          var Bfin: RDD[List[(String,Int)]] = sc.emptyRDD
          for(Bze <- 0 to (BallClusters(Bzwi).size-1) ){
            val Bwc = BdocuWithIndex.filter(x=> x._2 == BallClusters(Bzwi)(Bze)).map(x => x._1)
            Bfin = Bfin ++ Bwc
          }
          val Bboom = Bfin.collect().toList.flatten.sortBy(x => -x._2)
          val BTop10F = sc.parallelize(Bboom).map(x=>x._1).collect().toList.take(10) 
          print("TOP 10 KOKO: " + BTop10F + "\n")
          BFinalResults += BTop10F
        }
        
        /*
        print("TA FINAL RESULTS!!!!!! " + "\n")
        print(BFinalResults)
        print("ACCESSING THE RESULTS OF ALL THE CLUSTERS" +"\n")
        print(BFinalResults(0) + "\n")
        print(BFinalResults(1) + "\n")
        print(BFinalResults(2) + "\n")
        print(BFinalResults(3) + "\n")
        print(BFinalResults(4) + "\n")
        print(BFinalResults(5) + "\n")
        print(BFinalResults(6) + "\n")
        print(BFinalResults(7) + "\n") 
        print("SSE LIST NA DOUMEN POSO EN TO SSE GIA KATHE CLUSTER"+ "\n")
        print(BSSEList.toList + "\n")

       */
        var BTotCost = 0.0
        print(BSSEList.toList.size)
        for(Bella <- (0 to BSSEList.toList.size -1)){
          BTotCost = BTotCost + BSSEList(Bella)
        }
          
  		  
  		  val pw = new PrintWriter(new File("Georgios_Iliadis_B_small_8_20.json" ))
        pw.write("algorithm : Bisecting K-Means," + "\n" + "WSSE: " + BTotCost + ",")
          pw.write("clusters:[ " + "\n")
          for(fi <- 0 to 7){
            val numeron = fi+1
            pw.write("{" +"\n" + "\t" + "id: " + numeron + "," + "\n")
            pw.write("\t" + "size: " + BallClusters(fi).size + "," + "\n")
            pw.write("\t" + "error: " + BSSEList(fi) + "," + "\n")
            pw.write("\t" + "terms: " + BFinalResults(fi) + "\n")
            pw.write("}," + "\n")
          }
          pw.write("]")
          pw.close
  		 
           
           
    }
        
        
}
   
