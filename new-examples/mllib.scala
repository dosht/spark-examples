import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.{ HashingTF, Normalizer }
import org.apache.spark.mllib.classification.{ LogisticRegressionWithSGD, NaiveBayes }
import org.apache.spark.mllib.tree.DecisionTree

//val path = "data/sample"
val path = "data/all"
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
val trainData = nonSpamFeatures.map(x => LabeledPoint(0, x)) union spamFeatures.map(x => LabeledPoint(1, x))
val norm = new Normalizer()
val trainDataNormalize = trainData.map(l => LabeledPoint(l.label, norm.transform(l.features)))

// Split
//val Array(trX, teX) = trainData.randomSplit(Array(0.8, 0.2), seed = 11L)
val Array(trX, teX) = trainDataNormalize.randomSplit(Array(0.8, 0.2), seed = 11L)
trX.cache()

val model = LogisticRegressionWithSGD.train(trX, 100)
val model = NaiveBayes.train(trX, lambda = 1.0, modelType = "multinomial")
val model = DecisionTree.trainClassifier(trX, 2, Map[Int, Int](), "gini", 5, 32)

teX.map(l => l.label - model.predict(l.features)).map(scala.math.pow(_, 2)).mean

teX.filter(l => l.label == model.predict(l.features)).count / teX.count.toFloat

