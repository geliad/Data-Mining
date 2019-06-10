package Georgios.HW2

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import java.io._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.ml.feature.StringIndexer


object ModelBasedCF {
  
   def main(args: Array[String]) {
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      val sc = new SparkContext("local[*]", "Task1")
      val startTime = System.nanoTime()
      //val readTrainFile = args(0)
      //val readTestFile = args(1)
      //Where there is "train_review.csv" replace with readTrainFile
      //Where there is "test_review.csv" replace with readTestFile
      val train = sc.textFile("train_review.csv") 
      val test = sc.textFile("test_review.csv")
      
      //Read in data, remove first row
      val headerTrain = train.first()
      val headerTest = test.first()
      val trainData = train.filter(row => row!= headerTrain)
      val testData = test.filter(row => row!= headerTest)
       
      //Create Ratings data
      val testDAT = testData.map(line => line.split(",")).map{x => ((x(0),x(1)),x(2))}
      val trainDAT = trainData.map(line => line.split(",")).map{x => ((x(0),x(1)),x(2))}
      val DATA = trainDAT.subtractByKey(testDAT).map{ case ((user, product), rate) => (user, product, rate)}
      val trainRatings = DATA.map{ case (user, product, rate) =>Rating(user.hashCode(), product.hashCode(), rate.toDouble)}
      
      val testRatings = testDAT.map{ case ((user,product),rate) =>Rating(user.hashCode(), product.hashCode(),rate.toDouble)}
      
      val trainDAT2 = trainDAT.map{case ((user, product), rate) => ((user.hashCode(), product.hashCode()), rate.toDouble)}
      val testDAT2 = testDAT.map{case ((user, product), rate) => ((user.hashCode(), product.hashCode()), rate.toDouble)}
            
      //Build recommend system using ALS  
      val rank = 2//10
		  val numIterations = 15//15
		  val lambda = 0.25//0.28
		  val model = ALS.train(trainRatings, rank, numIterations,lambda)
		  
		  //Calculate Average for each user
		  val avg = trainDAT.map{ case ((user, product), rate) => (user.hashCode(), (product.hashCode(),rate.toDouble,1) ) }.reduceByKey((x,y) => (y._1 +x._1 , y._2 + x._2, x._3 + y._3))
      val UserAver = avg.map(x=> ((x._1),(x._2._2/x._2._3))).map{case((user),(avg)) =>
            if (avg <= 0) ((user), (0.0))
            else if (avg >= 5) ((user), (5.0))
            else ((user), (avg))}
    					          
      val ratingTest = testDAT.map{case((user, product), rate) =>Rating(user.hashCode(), product.hashCode(), rate.toDouble)}
      val userProductsTest = ratingTest.map { case Rating(user,product,rate)=>(user,product)}
		   
      //Predictions one with model, one with average ->the ones that are missing values
      val PredictionTest1 = model.predict(userProductsTest).map{case Rating(user, product, rate) => ((user, product),rate)}
      val PredMap = PredictionTest1.collect().toMap
      val Remaining = testDAT2.filter(x => !PredMap.keySet.contains(x._1))
      val PredictionAverage = Remaining.map{case((user, product), num) => (user, (product))}
          .join(UserAver)  
          .map{case(user,(product, aver)) => ((user,product), aver)}
      val PredictionJoined = PredictionTest1.++(PredictionAverage)
      val PredFinal = PredictionJoined.map{case((user, product), pred) =>
            if (pred <= 0) ((user, product), 0.0)
            else if (pred >= 5) ((user, product), 5.0)
            //else if (pred < 5 & pred > 0) ((user, product), (pred))
            else ((user, product), pred)
        } 
		   val rateAndPredFINAL = ratingTest.map{case Rating(user, product, rate) => ((user, product), rate)}.join(PredFinal)		  
		

		  //Calculate difference between ratings(r1) and predictions(r2)
      val difference = rateAndPredFINAL.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}
      val count1 = difference.filter(x => x._2 >= 0 && x._2 <1).count()
      val count2 = difference.filter(x => x._2 >= 1 && x._2 <2).count()
      val count3 = difference.filter(x => x._2 >= 2 && x._2 <3).count()
      val count4 = difference.filter(x => x._2 >= 3 && x._2 <4).count()
      val count5 = difference.filter(x => x._2 >= 4).count()
      
		  //Print results
      println(">= 0 and < 1: " + count1 )
      println(">= 1 and < 2: " + count2 )
      println(">= 2 and < 3: " + count3 )
      println(">= 3 and < 4: " + count4 )
      println(">= 4: " + count5)
      
      
      val MSEtest = rateAndPredFINAL.map { case ((user, product), (r1, r2)) =>val err = (r1 - r2)
		  err * err}.mean()
      println("RMSE: " + Math.sqrt(MSEtest))
      
      val endTime = System.nanoTime()
      val timePrint = (endTime - startTime) / 1000000000
      print("Time: " +  timePrint  + " sec")
    
		  //Write results to .txt file
      val result = rateAndPredFINAL.collect().sortBy(x => (x._1._1, x._1._2, true))
          .map(x=> x._1._1.toString +  "," + x._1._2.toString + "," + x._2._2.toString)
      val outputF = "Georgios_Iliadis_ModelBasedCF.txt"
      val file = new FileWriter(outputF)
      result.map(x => file.write(x + "\n"))
      file.close()

   }
   
}