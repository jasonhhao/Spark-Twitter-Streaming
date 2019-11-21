package spark.streaming.twitter

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

object TwitterSentimentScore extends App {

  import Utils._

  val sparkConfiguration = new SparkConf().
    setAppName("spark-twitter-stream").
    setMaster(sys.env.get("spark.master").getOrElse("local[*]"))
  val sparkContext = new SparkContext(sparkConfiguration)

  val streamingContext = new StreamingContext(sparkContext, Seconds(10))  //window size every 10s

  // filter out english words only
  val tweets: DStream[Status] =
    TwitterUtils.createStream(streamingContext, None).filter(_.getLang == "en")

  // Since these lists are pretty small
  // it can be worthwhile to broadcast those across the cluster so that every
  // executor can access them locally
  val uselessWords = sparkContext.broadcast(load("/stop-words.dat"))
  val positiveWords = sparkContext.broadcast(load("/pos-words.dat"))
  val negativeWords = sparkContext.broadcast(load("/neg-words.dat"))

  val textAndSentences: DStream[(TweetText, Sentence)] =
  tweets.
    map(_.getText).
    map(tweetText => (tweetText, wordsOf(tweetText)))

  // Apply several transformations that allow us to keep just meaningful sentences
  val textAndMeaningfulSentences: DStream[(TweetText, Sentence)] =
    textAndSentences.
      mapValues(toLowercase).
      mapValues(keepActualWords).
      mapValues(words => keepMeaningfulWords(words, uselessWords.value)).
      filter { case (_, sentence) => sentence.length > 0 }

  // Compute the score of each sentence and keep only the non-neutral ones
  val textAndNonNeutralScore: DStream[(TweetText, Int)] =
    textAndMeaningfulSentences.
      mapValues(sentence => computeScore(sentence, positiveWords.value, negativeWords.value)).
      filter { case (_, score) => score != 0 }


  textAndNonNeutralScore.map(makeReadable).print
  textAndNonNeutralScore.saveAsTextFiles("tweets", "json")
  streamingContext.start()
  streamingContext.awaitTermination()

}