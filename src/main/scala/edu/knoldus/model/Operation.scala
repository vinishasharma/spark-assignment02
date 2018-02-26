package edu.knoldus.model

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object Operation {
  val logger = Logger.getLogger(this.getClass)
  val conf = new SparkConf().setMaster("local").setAppName("spark-assignment")
  val sc = new SparkContext(conf)
  val customerRdd = sc.textFile("/home/knoldus/IdeaProjects/spark-assignment02/customerData.txt")
  val salesRdd = sc.textFile("/home/knoldus/IdeaProjects/spark-assignment02/salesData.txt")
  val customer = customerRdd.flatMap(customerElement => customerElement.split('\n')).map(element => element.split('#')).map(custValue => (custValue(0), Customer(custValue(0), custValue(1), custValue(2), custValue(3))))
  val sales = salesRdd.map(element => {
    val splittedElements = element.split('#')
    (splittedElements(1),Order(splittedElements(0).toLong, splittedElements(1), splittedElements(2).toDouble))
  })

  def getSalesRecords: Unit = {
    val resultRdd = customer join sales
    val yearlyRecord = resultRdd
      .map(element => ((element._2._1.city, element._2._2.date.getYear + 1900, element._1),element._2._2.sales))
      .reduceByKey(_ + _)
      .map(mapElement => s"${mapElement._1._1}#${mapElement._1._2}###${mapElement._2}")

    val monthlyRecord = resultRdd.
      map(element => ((element._2._1.city, element._2._2.date.getYear + 1900, element._2._2.date.getMonth + 1, element._1),element._2._2.sales))
      .reduceByKey(_ + _)
      .map(mapElement => s"${mapElement._1._1}#${mapElement._1._2}#${mapElement._1._3}##${mapElement._2}")

    val dailyRecord = resultRdd
      .map(element => ((element._2._1.city, element._2._2.date.getYear + 1900, element._2._2.date.getMonth + 1, element._2._2.date.getDate, element._1),element._2._2.sales))
      .reduceByKey(_ + _)
      .map(custElement => s"${custElement._1._1}#${custElement._1._2}#${custElement._1._3}#${custElement._1._3}#${custElement._2}")

    val yearRdd = yearlyRecord.collect.toList
    val monthRdd = monthlyRecord.collect.toList
    val dailyRdd = dailyRecord.collect.toList

    val finalRecord = yearRdd.union(monthRdd).union(dailyRdd)
    val res = sc.parallelize(finalRecord)
    res.repartition(1).saveAsTextFile("/home/knoldus/spark_kip/customerResult")
    logger.info(s"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n${finalRecord}\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")


  }


}
