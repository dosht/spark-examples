from pyspark import SparkContext
import os

logFile = "%s/README.md" % os.environ.get("SPARK_HOME", "../incubator-spark")
sc = SparkContext("local", "Simple PyApp")

logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print "Lines with a: %i, lines with b: %i" % (numAs, numBs)


