from pyspark import SparkContext

logFile = "/home/msameh/osrc/incubator-spark/README.md"
sc = SparkContext("local", "Simple PyApp")

logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print "Lines with a: %i, lines with b: %i" % (numAs, numBs)


