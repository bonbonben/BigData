# You need to import everything below
import pyspark
from pyspark import SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql import Window
from pyspark.sql.functions import udf

from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MulticlassMetrics

import lime
from lime import lime_text
from lime.lime_text import LimeTextExplainer

import numpy as np
import csv
sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("hw3") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

########################################################################################################
# Load data
categories = ["alt.atheism", "soc.religion.christian"]
LabeledDocument = pyspark.sql.Row("category", "text")

def categoryFromPath(path):
    return path.split("/")[-2]

def prepareDF(typ):
    rdds = [sc.wholeTextFiles("/user/tbl245/20news-bydate-" + typ + "/" + category)\
              .map(lambda x: LabeledDocument(categoryFromPath(x[0]), x[1]))\
            for category in categories]
    return sc.union(rdds).toDF()

train_df = prepareDF("train").cache()
test_df  = prepareDF("test").cache()

#####################################################################################################
""" Task 1.1
a.      Compute the numbers of documents in training and test datasets. Make sure to write your code here and report
    the numbers in your txt file.
b.      Index each document in each dataset by creating an index column, "id", for each data set, with index starting at 0.

"""
# Your code starts here
train_df.createOrReplaceTempView("train_df")
test_df.createOrReplaceTempView("test_df")

spark.sql("select count(*) from train_df").show()
spark.sql("select count(*) from test_df").show()

train_df = train_df.withColumn("id",row_number().over(Window.orderBy(monotonically_increasing_id()))-1)
train_df.createOrReplaceTempView("train_df")
train_df.show(5)

test_df = test_df.withColumn("id",row_number().over(Window.orderBy(monotonically_increasing_id()))-1)
test_df.createOrReplaceTempView("test_df")
test_df.show(5)

########################################################################################################
# Build pipeline and run
indexer   = StringIndexer(inputCol="category", outputCol="label")
tokenizer = RegexTokenizer(pattern=u'\W+', inputCol="text", outputCol="words", toLowercase=False)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
idf       = IDF(inputCol="rawFeatures", outputCol="features")
lr        = LogisticRegression(maxIter=20, regParam=0.001)

# Builing model pipeline
pipeline = Pipeline(stages=[indexer, tokenizer, hashingTF, idf, lr])

# Train model on training set
model = pipeline.fit(train_df)   #if you give new names to your indexed datasets, make sure to make adjustments here

# Model prediction on test set
pred = model.transform(test_df)  # ...and here

# Model prediction accuracy (F1-score)
pl = pred.select("label", "prediction").rdd.cache()
metrics = MulticlassMetrics(pl)
metrics.fMeasure()

#####################################################################################################
""" Task 1.2
a.      Run the model provided above.
    Take your time to carefully understanding what is happening in this model pipeline.
    You are NOT allowed to make changes to this model's configurations.
    Compute and report the F1-score on the test dataset.
b.      Get and report the schema (column names and data types) of the model's prediction output.

"""
# Your code for this part, IF ANY, starts here
f1Score = metrics.fMeasure()
print("F1-score = %s" % f1Score)
columnNames = pred.schema.names
print("column names:", columnNames)
dataTypes = [f.dataType for f in pred.schema.fields]
print("data types:", dataTypes)

#######################################################################################################
#Use LIME to explain example
class_names = ['Atheism', 'Christian']
explainer = LimeTextExplainer(class_names=class_names)

# Choose a random text in test set, change seed for randomness
test_point = test_df.sample(False, 0.1, seed = 10).limit(1)
test_point_label = test_point.select("category").collect()[0][0]
test_point_text = test_point.select("text").collect()[0][0]

def classifier_fn(data):
    spark_object = spark.createDataFrame(data, "string").toDF("text")
    pred = model.transform(spark_object)   #if you build the model with a different name, make appropriate changes here
    output = np.array((pred.select("probability").collect())).reshape(len(data),2)
    return output

exp = explainer.explain_instance(test_point_text, classifier_fn, num_features=6)
print('Probability(Atheism) =', classifier_fn([test_point_text])[0][1])
print('Probability(Christian) =', classifier_fn([test_point_text])[0][0])
print('True class: %s' % class_names[categories.index(test_point_label)])
exp.as_list()

#####################################################################################################
"""
Task 1.3 : Output and report required details on test documents with IDs 0, 275, and 664.
Task 1.4 : Generate explanations for all misclassified documents in the test set, sorted by conf in descending order,
           and save this output (index, confidence, and LIME's explanation) to netID_misclassified_ordered.csv for submission.
"""
# Your code starts here
pred.createOrReplaceTempView("pred")
temp1 = spark.sql("SELECT category, probability, prediction, words FROM pred WHERE id in ('0', '275', '664')")
temp1.show()

def test_case(id):
        test_point = spark.sql("select * from test_df where id = {0}".format(id))
        test_point_label = test_point.select("category").collect()[0][0]
        test_point_text = test_point.select("text").collect()[0][0]
        exp = explainer.explain_instance(test_point_text, classifier_fn, num_features=6)
        print('Probability(Atheism) =', classifier_fn([test_point_text])[0][1])
        print('Probability(Christian) =', classifier_fn([test_point_text])[0][0])
        print('True class: %s' % class_names[categories.index(test_point_label)])
        return exp.as_list()

test_case(0)
test_case(275)
test_case(664)

temp2 = spark.sql("SELECT id, probability FROM pred WHERE label != prediction")
temp2.createOrReplaceTempView("temp2")
temp2.show()

misclassified_id = []
contributed_word = []
temp = pred.select(pred['id'], pred['probability']).where("label != prediction").collect()
for info in temp:
    misclassified_id.append([info[0], abs(info[1][0]-info[1][1])])
misclassified_id = sorted(misclassified_id, key=lambda x: -x[1])

with open('misclassified_ordered.csv', mode='w') as csv_file:
    for id in misclassified_id:
        temp = test_case(id[0])
        for i in range(len(temp)):
            contributed_word.append(temp[i])
        csv_file.write("{}, {}, {}\n".format(id[0],id[1],temp))

########################################################################################################
""" Task 1.5
Get the word and summation weight and frequency
"""
# Your code starts here
all_weight = sc.parallelize(contributed_word)
total = all_weight.map(lambda x: (x[0],abs(x[1])))
result = total.reduceByKey(lambda x, y: x + y)
key_only = total.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
result = result.join(key_only)
result = result.map(lambda x: (x[0],(x[1][1]),x[1][0]/x[1][1]))
result = result.sortBy(lambda x: (-x[1],-x[2]))

def output_format(x):
    return "{}, {}, {}".format(x[0],x[1],x[2])

task1_5 = result.map(lambda x: output_format(x))
task1_5.saveAsTextFile("words_weight.csv")

########################################################################################################
""" Task 2
Identify a feature-selection strategy to improve the model's F1-score.
Codes for your strategy is required
Retrain pipeline with your new train set (name it, new_train_df)
You are NOT allowed make changes to the test set
Give the new F1-score.
"""

#Your code starts here
task2_weight = sc.parallelize(contributed_word[0:len(contributed_word)-12])
total = task2_weight.map(lambda x: (x[0],abs(x[1])))
result = total.reduceByKey(lambda x, y: x + y)
key_only = total.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
result = result.join(key_only)
result = result.map(lambda x: (x[0],(x[1][1]),x[1][0]/x[1][1]))
result = result.sortBy(lambda x: (-x[1],-x[2]))

word_list = result.collect()
word_list = [word[0] for word in word_list]

candidate = []
for i in range(len(word_list)):
	remove = udf(lambda x: x.replace(word_list[i],''), StringType())
	new_train_df = train_df.select(train_df['category'],remove(train_df['text']).alias('text'),train_df['id'])
	modify_model = pipeline.fit(new_train_df)
	new_pred = modify_model.transform(test_df)
	new_pl = new_pred.select("label", "prediction").rdd.cache()
	new_metrics = MulticlassMetrics(new_pl)
	new_metrics.fMeasure()
	candidate.append([word_list[i], new_metrics.fMeasure()])

for i in range(len(candidate)):
	if candidate[i][1] > 0.95:
		print('Remove candidate =', i, candidate[i])

#Remove candidate = 3 ['Carl', 0.9511854951185496]
#Remove candidate = 11 ['Asimov', 0.9511854951185496]
#Remove candidate = 23 ['ctron', 0.9511854951185496]
#Remove candidate = 27 ['Return', 0.9511854951185496]
#Remove candidate = 29 ['Path', 0.9511854951185496]
#Remove candidate = 58 ['Revelation', 0.9511854951185496]

remove = udf(lambda x: x.replace(word_list[3],'').replace(word_list[11],'').replace(word_list[23],'').replace(word_list[27],'').replace(word_list[29],'').replace(word_list[58],''),StringType())
new_train_df = train_df.select(train_df['category'],remove(train_df['text']).alias('text'),train_df['id'])
modify_model = pipeline.fit(new_train_df)
new_pred = modify_model.transform(test_df)
new_pl = new_pred.select("label", "prediction").rdd.cache()
new_metrics = MulticlassMetrics(new_pl)
new_metrics.fMeasure()

new = new_pred.select(new_pred['id']).where("label != prediction").collect()
old = pred.select(pred['id']).where("label != prediction").collect()
new_f1Score = new_metrics.fMeasure()
print("F1-score = %s" % new_f1Score)

for i in range(len(new)):
	print(new[i][0])

new_pred.createOrReplaceTempView("new_pred")
spark.sql("Select pred.id from pred where pred.label != pred.prediction Except Select new_pred.id from new_pred where new_pred.label != new_pred.prediction").show()
