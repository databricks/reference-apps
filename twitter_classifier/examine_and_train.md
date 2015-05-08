# Part 2: Examine Tweets and Train a Model

The second program examines the data found in tweets and trains a language classifier using K-Means clustering on the tweets:

* [Examine](examine.md) - Spark SQL is used to gather data about the tweets -- to look at a few of them, and to count the total number of tweets for the most common languages of the user.
* [Train](train.md) - Spark MLLib is used for applying the K-Means algorithm for clustering the tweets.  The number of clusters and the number of iterations of algorithm are configurable.  After training the model, some sample tweets from the different clusters are shown.

See [here](run_part2.md) for the command to run part 2.
