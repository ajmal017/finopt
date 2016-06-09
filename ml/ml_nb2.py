from pyspark import SparkContext
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint


def parseLine(line):
    parts = line.split(',')
    label = float(parts[0])
    features = Vectors.dense([float(x) for x in parts[1].split(' ')])
    return LabeledPoint(label, features)


def train():
    sc = SparkContext(appName= 'nb_test')    
    data = sc.textFile('../dat/^HSI-^DJI_^FCHI_^FVX_^FTSE_VNQ_QQQ_GOOG_BAC-').map(parseLine)
    
    # Split data aproximately into training (60%) and test (40%)
    training, test = data.randomSplit([0.7, 0.3], seed=0)
    print training.collect()
    # Train a naive Bayes model.
    model = NaiveBayes.train(training, 1.0) #, "bernoulli")
    predictionAndLabel = test.map(lambda p: (model.predict(p.features), p.label))
    accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()
    print '**** ACCURACY', accuracy
    # Save and load model
    #model.save(sc, "../../target/myNaiveBayesModel")


def predict():
# Make prediction and test accuracy.
    sc = SparkContext(appName= 'nb_test')    
    sameModel = NaiveBayesModel.load(sc, "../../target/myNaiveBayesModel")
    data = sc.textFile('../../data/mllib/sample_naive_bayes_data.txt').map(parseLine)
    
    # Split data aproximately into training (60%) and test (40%)
    training, test = data.randomSplit([0.1, 0.9], seed=0)
    print test.collect()
    predictionAndLabel = test.map(lambda p: (sameModel.predict(p.features), p.label))
    print predictionAndLabel.collect()
    accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()
    print accuracy

if __name__ == '__main__':

    train()
