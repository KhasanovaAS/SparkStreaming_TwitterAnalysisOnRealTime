import com.google.gson.Gson
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils


object TwitterStreamJSON {

  def main(args: Array[String]): Unit = {

    val outputDirectory = args(0)

    val conf = new SparkConf()
    conf.setAppName("SparkTwitter")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(30))

    // Configure your Twitter credentials
    val apiKey = "0Xhqk5XkGuWpQUiAc3pIk8Wzl"
    val apiSecret = "TpQ8yj7ze6K9CnCOKj6QYhCGe3wLZOkf9znIFDvot1oBJwh3jd"
    val accessToken = "924164191400931328-Xij0b3Ngex03vDWkROhSDpBDZOPgb2Z"
    val accessTokenSecret = "waMiFai3NF40SjFjozdFhtAASsEye9bOE1bzjMV8GNsj0"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    //in order to avoid IO.Exception
    System.setProperty("hadoop.home.dir", "C:\\HADOOP\\");

    // Create Twitter Stream in JSON
    val tweets = TwitterUtils
      .createStream(ssc, None)
      .map(new Gson().toJson(_))

    val numTweetsCollect = 10000L
    var numTweetsCollected = 0L
    val outputDiretory="Data/tweets_[0-9]*/part-00000"
    //Save tweets in file
    /*tweets.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.coalesce(1)
        outputRDD.saveAsTextFile(outputDirectory+"/"+time)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsCollect) {
          System.exit(0)
        }
      }
    })*/

    //HDFS_directory+"/gdelt/2015[0-9]*.export.csv"
    tweets.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(1)
        outputRDD.saveAsTextFile(outputDirectory+"/tweets_"+ time.milliseconds.toString)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsCollect) {
          System.exit(0)
        }
      }
    })





    ssc.start()
    ssc.awaitTermination()
  }
}