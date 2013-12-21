Word Count - The first example for spark
========================================

1. Define a system variable called SPARK_HOME that points to your clone of spark source code in your file system
```
export SPARK_HOME="/osrc/incubator-spark" # a location in your file system
```

2. Create a jar that will be used by spark
```
sbt package
```

3. Run the example
```
sbt run
```

-----------------------------

Tweet to [@dosht](https://twitter.com/intent/tweet?screen_name=dosht)
