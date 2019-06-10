package Georgios.HW3

import util.control.Breaks._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import java.io._
import scala.collection.mutable.ListBuffer


object SON {
  
   def AprioriFirst(Basket: List[List[String]], numPartitions: Int, support: Int): Set[Set[String]] = {
      
      val Threshold = support / numPartitions 
      val hashMap = Basket.flatten.toSet.zipWithIndex.toMap
      //Map[String, Int] 
      val ItemsInt = Basket.map(basket => basket.map(x => hashMap(x))).flatten
      //List[Int]
      var returns : Set[Set[Int]] = Set()
      //var returnn = Set.empty[Set[Int]]
      
      val FinalSet = ItemsInt.groupBy(identity).mapValues(_.size).filter(x => x._2 >= Threshold)
        .map(x => x._1).toSet  
      //arithmoi anti gia lexeis pou kamnoun match sto hashMap!!
      FinalSet.foreach{item => returns += Set(item)}
      val fixHashMap = hashMap.map(_.swap)
      return returns.map(x => x.map(y => fixHashMap(y).toString))
   }
   
   def AprioriSecond(Data: List[List[String]], numPartionn: Int ,support: Int, Candidate: Set[Set[String]]): Set[Set[String]] = {
   
    val Threshold = support / numPartionn
    //val CandidateSet = Candidate.flatten
    val IntersectSets = Data.flatten.toSet.intersect(Candidate.flatten)
    //Set[String]
    //val hashMap = Data.flatten.toSet.intersect(Candidate.flatten).zipWithIndex.toMap
    val hashMap = IntersectSets.zipWithIndex.toMap
    //Map[String,Int]
    val ItemInt = Data.map(basket => basket.filter(x => IntersectSets.contains(x)).map(y => hashMap(y)))
    //Candidate = Set[Set[String]
    val Candidates = Candidate.filter(x => x.forall(IntersectSets.contains)).map{x => x.map(y => hashMap(y))}
      
    //Get results, gia kathe ena pou kati kamnei koitaxe na men to parakamnei!
    //Set[set[string]] thelw sto telos alla dame prepei na to sasw!
    val returns = Candidates.map {
      elem => {
        var count = 0
        ItemInt.foreach {x => if (elem.forall(x.contains)) {
                                count = count + 1
                              }
                         }
                          (elem, count)
                  }
      }
      .filter(x => x._2 >= Threshold).map(x => x._1)
    
    val fixHashMap = hashMap.map(_.swap)
    return returns.map(x => x.map(y => fixHashMap(y).toString))
  }
  
  def main(args: Array[String]) {
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      val conf = new SparkConf().setAppName("SON-Algo-Georgios-Iliadis-INF553").setMaster("local[*]")
      //conf.set("spark.network.timeout", "600s")
      //conf.set("spark.sql.broadcastTimeout", "36000s")
      //conf.set("spark.rpc.askTimeout", "600s")
      //conf.set("spark.yarn.executor.memoryOverhead", "809")
     
      val sc = new SparkContext(conf)
      val startTime = System.nanoTime()
      val inputFile = sc.textFile(args(0))
      val support = args(1).toInt
      //val support = 1000
      //val inputFile = sc.textFile("yelp_reviews_small.txt")  
      val baskets = inputFile.map(line =>line.split(",")).map(x=> (x(0), Set(x(1).toString))) 
        .reduceByKey((x, y) => x ++ y).mapValues(x => x.toList).values

      //val nameOutputFile = "Georgios_Iliadis_SON_"+ inputFile.toString.substring(inputFile.toString.lastIndexOf('/') + 1, inputFile.toString.length - 4) + support + ".txt"
      val nameOutputFile2 = args(2)
      //val nameOutputFile2 = "GeorgiosDeTiGinetai.txt"
      val file = new File(nameOutputFile2)
      val numberPartitions = baskets.getNumPartitions 
      val pw = new BufferedWriter(new FileWriter(file))
      //Set[Set[String]]
      //Rdd -> Set
      var CandidateSets = baskets
        .mapPartitions(x => AprioriFirst(x.toList, numberPartitions, support).toIterator)
        .treeAggregate(Set[Set[String]]())(
            (a, b) => a.union(Set(b)), (x, y) => x ++ y)
  
      FinalOutputWrite(baskets, CandidateSets, support, pw)
      var Cardinality = 2
      
      //Loop dame me ta pairs na kamnoun increase until no more candidates!
      //while(CandidateSets is not empty) -> calculate newcandidates kai add them an en to temp entaxei ston kairo.
      while(CandidateSets.nonEmpty) {
          var newCand : Set[Set[String]] = Set()
          CandidateSets.map{a => CandidateSets.map{b => {
                val temp = a ++ b
                if ((temp.size == a.size + 1)) {
                  newCand = newCand + temp
                }
              }
            }
          }
          CandidateSets  = baskets
            .mapPartitions(x => AprioriSecond(x.toList, numberPartitions, support, newCand).toIterator)
            .treeAggregate(Set[Set[String]]())(
                (a, b) => a.union(Set(b)), (x,y) => x++y)
                
          FinalOutputWrite(baskets , CandidateSets , support, pw)
          Cardinality = Cardinality + 1
      }
  
     val endTime = System.nanoTime()
     pw.close()
     val timePrint = (endTime - startTime) / 1000000000
     print("Time: " +  timePrint  + " sec")    
  }
  
  def FinalOutputWrite(Basket: RDD[List[String]], Candidates: Set[Set[String]], support: Int, buffWri: BufferedWriter)={
    //val FinalRes = Candidates.map(itemSet => {var c = 0
    // Baske.map(x => { if (itemSet.forall(x.contains)) { count =+ 1 }}
                       
    val FinalResult = Basket.mapPartitions(x => FinalCount(x.toList, Candidates).toIterator)
      .reduceByKey((x, y) => x+y).filter(x => x._2 >= support).collect()
    //Sort
    val FinalSort = FinalResult.map(x => x._1).groupBy(_.size)
      .map{case (x, y) => {
            val arrayList = y.map{str => str.toList.sorted}
            (x, arrayList.sortBy(_.toString))
          }
      }
      .toList.sortBy{case (x,y) => x}
    
    var pr = ""
    
    FinalSort.map{case (intege, arrayList)=>
        pr = ""
        arrayList.map{x => {pr += "(" 
            x.map{y => pr += y + ", "}
            pr = pr.substring(0, pr.length - 2) + "), "  
          }
        }
        buffWri.write(pr.substring(0, pr.length - 2) + "\n" + "\n")
    }
  }
  
  def FinalCount(Data: List[List[String]], Candidate: Set[Set[String]]): Set[(Set[String], Int)] = {
    val FileCount = Candidate.map{itemSet => {
        var count = 0
        Data.map(x => { if (itemSet.forall(x.contains)) {
                          count = count + 1
                        }
                      })
                        (itemSet, count)
                      }
                    }
    return FileCount
  }

   /*
       * Bhmata gia to Apriori:
       * 1) Ebre oulla ta canditates C1- (hey, boom, yo)
       * 2) Count the candidates an en parapano pou ta s(de mes ta baskets) -> L1 = (hey,boom)
       * 3) Ebre all candidates for pairs C2 = ((hey, boom))
       *    Ypologize to C2 apo to L1-> ara to yo en mporei na eni!!
       * 4) Ebre L2, check poses fores yparxoun mes ta baskets > s.
       * 5) Ebre C3 apo to L2 kai L1
       * 6) Ebre L3 
       * ===> Paizei loopa h fash 
       * 
       * Two final Methods: findCount - calulcate kai sigkrine me to support threshold
       * 									  writeToTxt - call findCount ,sort, write sto output
       * 
       * 
       * Apriori1 kai 2 with mapPartitions(chunk=>apriori(chunk!!)
       * Add cardinality until no more candidates are available.
       * Candidate set -> var not val so to change it in the loop!!
       * Increment cardin gia na evreis pairs,triples...
       */
  
 }
        
  
  
