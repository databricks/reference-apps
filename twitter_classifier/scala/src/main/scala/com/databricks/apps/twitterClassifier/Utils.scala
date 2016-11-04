package com.databricks.apps.twitterClassifier

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.HashingTF
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object Utils {
  val numFeatures = 1000
  val tf = new HashingTF(numFeatures)

  def maybeTwitterAuth: Some[OAuthAuthorization] = Some(new OAuthAuthorization(new ConfigurationBuilder().build))

  /** Create feature vectors by turning each tweet into bigrams of characters (an n-gram model)
    * and then hashing those to a length-1000 feature vector that we can pass to MLlib.
    * This is a common way to decrease the number of features in a model while still getting excellent accuracy
    * (otherwise every pair of Unicode characters would potentially be a feature). */
  def featurize(s: String): Vector = tf.transform(s.sliding(2).toSeq)
}
