import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD

val path = "data/sample"
val nonSpamFiles = sc.wholeTextFiles(s"$path/ham").cache()
val spamFiles = sc.wholeTextFiles(s"$path/spam").cache()
nonSpamFiles.take(5)

// Load
val nonSpam = nonSpamFiles.groupByKey().map { case (k, v) => v.reduce(_ + "\n" + _) }
val spam = nonSpamFiles.groupByKey().map { case (k, v) => v.reduce(_ + "\n" + _) }

// Features
val tf = new HashingTF(numFeatures = 10000)
val nonSpamFeatures = nonSpam.map(email => tf.transform(email.split(" ")))
val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
val trainData = 

// 

