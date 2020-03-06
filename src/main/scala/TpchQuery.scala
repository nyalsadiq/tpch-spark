package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
abstract class TpchQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(sc: SparkContext, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {

    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
  }

  def executeQueries(sc: SparkContext, schemaProvider: TpchSchemaProvider, queryNum: Int): ListBuffer[(String, Float)] = {

    // if set write results to hdfs, if null write to stdout
    // val OUTPUT_DIR: String = "/tpch"
    val OUTPUT_DIR: String = "file://" + new File(".").getAbsolutePath() + "/dbgen/output"

    val results = new ListBuffer[(String, Float)]

    var fromNum = 1;
    var toNum = 22;
    if (queryNum != 0) {
      fromNum = queryNum;
      toNum = queryNum;
    }

    for (queryNo <- fromNum to toNum) {
      val t0 = System.nanoTime()

      val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]

      outputDF(query.execute(sc, schemaProvider), OUTPUT_DIR, query.getName())

      val t1 = System.nanoTime()

      val elapsed = (t1 - t0) / 1000000000.0f // second
      results += new Tuple2(query.getName(), elapsed)

    }

    return results
  }

  def main(args: Array[String]): Unit = {

    var queryNum = 0;
    if (args.length > 0)
      queryNum = args(0).toInt

    val conf = new SparkConf().setAppName("Simple Application")

    val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")

    System.setProperty("com.amazonaws.services.s3.enableV4", "true");

    conf.set("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-2.amazonaws.com")
    conf.set("spark.hadoop.fs.s3a.access.key", accessKeyId)
    conf.set("spark.hadoop.fs.s3a.secret.key", secretAccessKey)

    val cachingScheme = System.getenv("SQL_CACHING_SCHEME")
    val cacheSize = System.getenv("SQL_CACHE_SIZE")
    conf.setSQLCachingScheme(cachingScheme)
    conf.setSQLCacheSize(cacheSize)

    System.out.println("Using: " + System.getenv("SQL_CACHING_SCHEME"))
    System.out.println("Using CacheManager cache of size: " + System.getenv("SQL_CACHE_SIZE"))

    val sc = new SparkContext(conf)
  
    // read files from local FS
    val INPUT_DIR = "s3a://honours-proj-tpc-h"

    // read from hdfs
    // val INPUT_DIR: String = "/dbgen"

    val schemaProvider = new TpchSchemaProvider(sc, INPUT_DIR)

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(sc, schemaProvider, queryNum)

    val outFile = new File("TIMES-" + cachingScheme + "-" + cacheSize + ".txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
    }

    bw.close()
  }
}
