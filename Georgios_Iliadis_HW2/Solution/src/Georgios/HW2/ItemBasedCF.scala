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

object ItemBasedCF {
  
  def main(args: Array[String]) {
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      val conf = new SparkConf().setMaster("local[*]").setAppName("Task2")
   
      val sc = new SparkContext(conf)
      val startTime = System.nanoTime()
     
      //val readTrainFile = args(0)
      //val readTestFile = args(1)
      //Where there is "train_review.csv" replace with readTrainFile
      //Where there is "test_review.csv" replace with readTestFile
      //DATA
      val train = sc.textFile("train_review.csv") 
      val test = sc.textFile("test_review.csv")
 
      val headerTrain = train.first()
      val headerTest = test.first()
      val trainData = train.filter(row => row!= headerTrain)
      val testData = test.filter(row => row!= headerTest)
      val testDAT = testData.map(line => line.split(",")).map{x => ((x(0),x(1)),x(2))}
      val trainDAT = trainData.map(line => line.split(",")).map{x => ((x(0),x(1)),x(2))}
      
      val DATA = trainDAT.subtractByKey(testDAT).map{ case ((user, product), rate) => (user.hashCode(), product.hashCode(), rate.toDouble)}
      val trainRatings = DATA.map{ case (user, product, rate) =>Rating(user.hashCode(), product.hashCode(), rate.toDouble)}
      val testRatings = testDAT.map{ case ((user,product),rate) =>Rating(user.hashCode(), product.hashCode(),rate.toDouble)}
      
      val testDAT0 = testDAT.map{case ((user, product), rate) => ((user.hashCode(), product.hashCode()),1)}
      val testDAT2 = testDAT.map{case ((user, product), rate) => ((user.hashCode(), product.hashCode()), rate.toDouble)}
      val trainDAT2 = trainDAT.map{case ((user, product), rate) => ((user.hashCode(), product.hashCode()), rate.toDouble)}


      val avg = DATA.map{ case (user, product, rate) => (product.hashCode(), (user.hashCode(),rate.toDouble,1) ) }.reduceByKey((x,y) => (y._1 +x._1 , y._2 + x._2, x._3 + y._3))
      //(Item, AverageRating)
      val ItemAver = avg.map(x=> ((x._1),(x._2._2/x._2._3)))
            .map(x => (x._1, Math.max(x._2, 0.0))).map(x => (x._1, Math.min(x._2, 5.0)))
					  
		  val avg11 = DATA.map{ case (user, product, rate) => (user.hashCode(), (product.hashCode(),rate.toDouble,1) ) }.reduceByKey((x,y) => (y._1 +x._1 , y._2 + x._2, x._3 + y._3))
      //(User, AverageRating)
      val UserAver = avg11.map(x=> ((x._1),(x._2._2/x._2._3))) 
            .map(x => (x._1, Math.max(x._2, 0.0))).map(x => (x._1, Math.min(x._2, 5.0)))
            
            
      val TEST = testDAT0.map{case((user, product), num) => 
            ((product),(user))}
      val TRAIN = trainDAT2.map{case((user,product),rate) => 
            (product, (user, rate))}
      val TEST1 = TEST.join(TRAIN).map{case(product, (useri ,(userj, ratej))) => 
            ((useri, userj),(product, ratej))}
     
      val Train1 = trainDAT2.map{case((user,product),rate) => (product, (user, rate))}
      //((user1,user2),(rating1, rating2))
      val Joined = Train1.join(Train1).map(x=>((x._2._1._1, x._2._2._1), (x._2._1._2, x._2._2._2)))    
      val uniqueJoinedRatings = Joined.filter(x=> x._1._1 != x._1._2)
 
      //Average
      val avg1 = uniqueJoinedRatings.map{ case ((useri, userj),(ratei, ratej)) => 
          ((useri, userj), (ratei ,1))}
          .reduceByKey((x,y) => (y._1 +x._1 , y._2 + x._2))
      val Aver1 = avg1.map(x=> ((x._1),(x._2._1 ,x._2._1/x._2._2)))
            .map(x => ((x._1), (x._2._1, Math.max(x._2._2, 0.0))))
            .map(x => ((x._1), (x._2._1, Math.min(x._2._2, 5.0))))

      val avg2 = uniqueJoinedRatings.map{ case ((useri, userj),(ratei, ratej)) => 
            ((useri, userj), (ratej ,1))}
            .reduceByKey((x,y) => (y._1 +x._1 , y._2 + x._2))
      val Aver2 = avg2.map(x=> ((x._1),(x._2._1 ,x._2._1/x._2._2)))
            .map(x => ((x._1), (x._2._1, Math.max(x._2._2, 0.0))))
            .map(x => ((x._1), (x._2._1, Math.min(x._2._2, 5.0))))   
      
      //(useri, userj),(ratei , averagei),(ratej, averagej)
      val AverFull = Aver1.join(Aver2)
      //Normalized
      val norm = AverFull.map{case((useri, userj),((ratei, avgi),(ratej, avgj))) => 
          ((useri, userj),((ratei - avgi),(ratej - avgj)))
      }     
      val userAvg = AverFull.map{case((useri, userj),((ratei, avgi),(ratej, avgj))) =>
          ((useri,userj),((avgi, avgj)))
      }
      

      //CALCULATION OF WEIGHT using the equation of weight from notes
      //Numerator calculation
      val Numerato = norm.map{case((useri, userj),(normRatei, normRatej)) => 
            ((useri, userj),(normRatei * normRatej))}
            .reduceByKey((x,y)=> x + y)
                                                                                                                    //  normRatej * normRatej
      //Denominator Calculation
      val Den = norm.map{case((useri, userj),(normRatei,normRatej)) => 
          ((useri, userj), Math.pow((normRatei),2) , Math.pow((normRatej),2))}
      //sum of both 
      val Deno = Den.map{case((useri, userj), squaredNormRatingi,  squaredNormRatingj) 
          => ((useri, userj), squaredNormRatingi)}
          .reduceByKey((x,y)=> x + y)
      val Denom = Den.map{case((useri, userj), squaredNormRatingi,  squaredNormRatingj) => 
            ((useri, userj), squaredNormRatingj)}
            .reduceByKey((x,y)=> x + y)
      val DenominatorFinal = Deno.join(Denom).map{case((useri, userj),(squaredNormRatingi,  squaredNormRatingj)) => 
            ((useri, userj), Math.sqrt(squaredNormRatingi) * Math.sqrt(squaredNormRatingj))}
      
      //(useri, userj) , (weight)
      val Weight = Numerato.join(DenominatorFinal).map{case((useri, userj), (numera, denomena)) =>
            if (denomena == 0)
                ((useri, userj), 0.0)
            else 
                ((useri, userj), (numera / denomena))
        }

      
      //Normalize test data- same as before
      //Finally create RDD with (useri, userj) (product, norm rate, weight)
      //(useri, userj),(product, normalized rating)
      val TESTweight = TEST1.join(userAvg).map{case((useri, userj),((product, ratej),(avgi, avgj))) =>
            ((useri, userj),(product, ratej - avgj))}
      //(Useri, Userj), ((item, normalized Rating), weight))
      val TESTweightFINAL = TESTweight.join(Weight)
        
      
      //Using weight,calculate prediction
      //Numerator = weight * rating
      //Denom = absolute value of weights
      val Pnumerator = TESTweightFINAL.map{case((useri, userj),((product, normRating), weigh)) => 
          ((useri, product), (normRating * weigh))}
      val pred1 = Pnumerator.reduceByKey((x,y)=> x + y)
      val Pdenominator = TESTweightFINAL.map{case((useri, userj),((product, normRating), weigh)) => 
          ((useri, product), Math.abs(weigh))}
      val pred2 = Pdenominator.reduceByKey((x,y)=> x + y)
      
      //(num / den) is prediction using prediction formula 
      val PredictionTest1 = pred1.join(pred2).map{case((user, product), (numera, denomena)) =>
            if (numera == 0 | denomena == 0) ((user, product), (0.toDouble))
            else ((user, product), (numera / denomena))
      }

      //Rest that have no rating - like task1 
      val Remaining = testDAT0.subtractByKey(PredictionTest1).map{case((user, product), (pred)) => ((user), (product))}
      //avg is prediction using average
      val PredictionAverage = Remaining.join(UserAver).map{case(user,(product, avg)) => ((user, product), avg)}
      val PredictionJoined = PredictionTest1.++(PredictionAverage)
        
      val PredFinal = PredictionJoined.map{case((user, product), pred) =>
           if (pred <= 0) ((user, product), 0.0)
           else if (pred >= 5) ((user, product), 5.0)
           //else if (pred < 5 & pred > 0) ((user, product), (pred))
           //else ((user,product), avg)
           else ((user, product), pred)
      }
        
      
      val rateAndPredFINAL = testDAT2.join(PredFinal).cache()			  
		  val difference = rateAndPredFINAL.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}.cache()
     
		  
    	val count1 = difference.filter(x => x._2 >= 0 && x._2 <1).count()
      val count2 = difference.filter(x => x._2 >= 1 && x._2 <2).count()
      val count3 = difference.filter(x => x._2 >= 2 && x._2 <3).count()
      val count4 = difference.filter(x => x._2 >= 3 && x._2 <4).count()
      val count5 = difference.filter(x => x._2 >= 4).count()
		  
      println(">= 0 and < 1: " + count1)
      println(">= 1 and < 2: " + count2)
      println(">= 2 and < 3: " + count3)
      println(">= 3 and < 4: " + count4)
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
      val outputF = "Georgios_Iliadis_ItemBasedCF.txt"
      val file = new FileWriter(outputF)
      result.map(x => file.write(x + "\n"))
      file.close()
      
			
  }
  
}