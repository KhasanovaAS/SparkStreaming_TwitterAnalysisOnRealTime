import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

object SparkTwitterStreaming2C {

  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("SparkTwitter")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    System.setProperty("hadoop.home.dir", "C:\\HADOOP\\");

    val ssc = new StreamingContext(sc, Seconds(1))

    // Configure your Twitter credentials
    val apiKey = "0Xhqk5XkGuWpQUiAc3pIk8Wzl"
    val apiSecret = "TpQ8yj7ze6K9CnCOKj6QYhCGe3wLZOkf9znIFDvot1oBJwh3jd"
    val accessToken = "924164191400931328-Xij0b3Ngex03vDWkROhSDpBDZOPgb2Z"
    val accessTokenSecret = "waMiFai3NF40SjFjozdFhtAASsEye9bOE1bzjMV8GNsj0"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Create Twitter Stream
    val stream = TwitterUtils.createStream(ssc, None)
    val tweets = stream.map(t => t.getText)

   // tweets.print()
    println("Initializing the KMeans model...")
    val modelInput = "KMEANS"
    val model = KMeansModel.load(sc, modelInput)
    val langNumber = 1


    val filtered = tweets.filter(t => model.predict(featurize(t)) == langNumber)
    filtered.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
