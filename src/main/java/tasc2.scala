import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object TestScala3 {

  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }


  def main(args: Array[String]): Unit = {

    //Create SparkContext
    val conf = new SparkConf()
    conf.setAppName("SparkTwitter")
    conf.setMaster("local[*]")
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.driver.memory", "1g")

    val sc = new SparkContext(conf)

    System.setProperty("hadoop.home.dir", "C:\\HADOOP\\");

    val sparkSession = SparkSession
      .builder()
      .appName("SparkTwitter")
      .master("local[*]")
      .getOrCreate()

    //Read json file to DF
    val tweetsDF = sparkSession.read.json("Data/tweets_[0-9]*/part-00000")


    // Show the Data
    //  tweetsDF.show()

    //  tweetsDF.printSchema();

    /* tweetsDF.createOrReplaceTempView("tweetTable")
     //Get the actor name and body of messages in Russian
     val Bd=sparkSession.sql(
       " SELECT text" +
         " FROM tweetTable WHERE text IS NOT NULL")
       .show(100)*/

    val Bd1 = tweetsDF.select("text").where("text is not null")
    //Bd1.show(100)

    val BdRDD = Bd1.toDF().rdd.map(r => r.toString())

    //Get the features vector
    val features = BdRDD.map(s => featurize(s))

    val numClusters = 10
    val numIterations = 40

    val modelOutput = "KMEANS"
    // Train KMenas model and save it to file
    val model: KMeansModel = KMeans.train(features, numClusters, numIterations)
    model.save(sparkSession.sparkContext, modelOutput)
  }
}
