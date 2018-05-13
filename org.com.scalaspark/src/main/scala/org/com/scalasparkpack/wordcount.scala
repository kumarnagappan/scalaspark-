package org.com.scalasparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object wordcount {
  def main(args:Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("word count").setMaster("local")
    val sc   = new SparkContext(conf)
    val file1 = sc.textFile("/home/datadotz/Documents/wordcountfile.txt").
    flatMap(rec => rec.split(",")).map(rec => (rec , 1)).map(rec => (rec,1))
    .reduceByKey((acc,value) => acc+value)
    
    file1.saveAsTextFile("/home/datadotz/Documents/wordcountfileout.txt")
    
    
  }
}