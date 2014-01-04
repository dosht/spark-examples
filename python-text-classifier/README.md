Text Classifier with SGD model and text HashingVerctorizer - over spark
========================================

This examples combines 2 other examples: [Out-of-core classification of text documents](http://scikit-learn.org/stable/auto_examples/applications/plot_out_of_core_classification.htm) and [SGD in Spark](https://gist.github.com/MLnick/4707012).

In this one scikit-learn stochastic gradient descent classifier [(SGDClassifier)](http://scikit-learn.org/stable/modules/generated/sklearn.linear_model.SGDClassifier.htm) and scikit-learn text features extractor [(HashingVectorizer)](http://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.HashingVectorizer.html) that converts text documents into a matrix of hashed tokens.

TODO: parallize reading and parsing docs and complete the README


1. Define a system variable called SPARK_HOME that points to your clone of spark source code in your file system
```
export SPARK_HOME="/osrc/incubator-spark" # a location in your file system
```

2. Add pyspark to your PYTHONPATH
```
export PYTHONPATH=$PYTHONPATH:~/osrc/incubator-spark/python # a location in your file system
```

3. Run the example
```
python SimpleApp.py
```

-----------------------------

Tweet to [@dosht](https://twitter.com/intent/tweet?screen_name=dosht)
