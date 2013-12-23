# Sparc Context
from pyspark import SparkContext
sc = SparkContext("local", "SVM-Classifier")

# Load
from sklearn.datasets import load_iris
iris = load_iris()
X = iris.data
y = iris.target
targets_names = iris.target_names

# Learn

from sklearn.svm import LinearSVC
def train(data):
    X = data['X']
    y = data['y']
    return LinearSVC('l2').fit(X, y)

# Merge 2 SVM Models
from sklearn.base import copy
def merge(left, right):
    new = copy.deepcopy(left)
    new.coef_ += right.coef_
    new.intercept_ += right.intercept_
    return new

 # Partition data
def dataPart(X, y, start, stop): return dict(X=X[start:stop, :], y=y[start:stop])

# Train the model and Validation
from sklearn import cross_validation
X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y)

# We will do this as if we have 3 files containing data and we need to parallize them
data = [dataPart(X_train, y_train, 0, 45), dataPart(X_train, y_train, 45, 75), dataPart(X_train, y_train, 75, 112)]

clf = sc.parallelize(data).map(train).reduce(merge)

print
print "The probability of expecting new samples: %s" % clf.score(X_test, y_test)
raw_input('Press any key to continue\n')
