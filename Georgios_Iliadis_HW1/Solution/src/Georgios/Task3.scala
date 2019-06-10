package Georgios

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._


object Task3 {
   def parseLine(line:String)= { 
        val fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
        val country = fields(3).toString
        //val ela = fields(4)
        val salary = fields(52) // exei pou oulla
        val salaryType = fields(53) // exei diafora
        (country, salary, salaryType)
    }
 
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Task3")   
    val input = sc.textFile("survey_results_public.csv")
    val header = input.first()
    val removeColumnNames = input.filter(row => row!= header)
    val parsedLines = removeColumnNames.map(parseLine)
    
    //FILTERING:
    val reportedSalaryValues = parsedLines.filter(x => x._2 != "NA")
    val reportedSalaryValues2 = reportedSalaryValues.filter(x => x._2 != 0)
    val reportedSalaryValues3 = reportedSalaryValues2.filter(x => x._2 != " ")
    val reportedSalaryValues4 = reportedSalaryValues3.filter(x => x._2 != "0").map(x=> (x._1.trim.replaceAll("\"", ""), x._2.trim.replace("\"", "").replace(",",""), x._3.trim.replace("NA","Yearly")))
     
    val weekly = reportedSalaryValues4.filter(x => x._3 == "Weekly").map(x =>(x._1, (x._2.toDouble * 52)))
    val monthly = reportedSalaryValues4.filter(x => x._3 == "Monthly").map(x =>(x._1, (x._2.toDouble * 12)))
    val annual = reportedSalaryValues4.filter(x => x._3 == "Yearly").map(x =>(x._1, (x._2.toDouble *1)))
    
    val joined1 = weekly ++ monthly ++ annual
    
    //MAX MIN
    val maax = joined1.map(x=>(x._1,x._2.toInt)).reduceByKey(math.max(_, _))
    val miin = joined1.map(x=>(x._1,x._2.toInt)).reduceByKey(math.min(_, _))

    
    //Here I created a an if condition where if the sum of the salaries is more than the max allowed integer value
    // then I will set the addition result to MAX_INT.
    // This is done to overcome any huge values for the average, because I will be using this sum for calculating the average.
    val MAX_INT = 2147483647.0 
    // Country Sum(Salaries) Count
    val Sum = joined1.map(x => (x._1,(x._2, 1))).reduceByKey((x,y) => ((if((x._1 + y._1)>2147483647.0) MAX_INT else (x._1 + y._1)),(x._2 + y._2)))
    // Country Average
    val resss = Sum.map(x => (x._1, BigDecimal(x._2._1/x._2._2).bigDecimal.doubleValue()))
    // Country Count
    val CouCount = Sum.map(x=> (x._1,x._2._2))
    
 
    val fmt = new java.text.DecimalFormat("#.00")
    // Country Max Min
    val resMinMax = maax.join(miin)
    // Country Count Average
    val res12 = CouCount.join(resss)
    //Country Max Min Count Average
    val all = resMinMax.join(res12)
    
    val all2 = all.collect().sorted
    
    /*
     * Print results
    for (x <- all2) {
      val avg = fmt.format(x._2._2._2)
      val countr = x._1
      val count = x._2._2._1
      val min = x._2._1._2
      val max = x._2._1._1
      println(s"$countr,$count,$min,$max,$avg")
    }
    */
    
    //Write to CSV
    val finalResults = all2.map(x=> (x._1, x._2._2._1,x._2._1._2,x._2._1._1,fmt.format(x._2._2._2)))
    
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    val theEnd = spark.createDataFrame(finalResults).toDF()
    theEnd.show()
    theEnd.coalesce(1).write.csv("Georgios_Iliadis_task3.csv")
     
    
  }
}