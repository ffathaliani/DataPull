import os
import sys

# Path for spark source folder
os.environ['SPARK_HOME'] = "/usr/local/Cellar/apache-spark/2.2.0/libexec"

# Append pyspark to Python Path
sys.path.append("/usr/local/Cellar/apache-spark/2.2.0/libexec/python")

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import dayofmonth, hour, dayofyear, month, year, weekofyear, format_number, date_format
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.recommendation import ALS
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.ml.linalg import Vectors
    from pyspark.ml.feature import VectorAssembler, VectorIndexer, OneHotEncoder, StringIndexer
    from pyspark.ml import Pipeline
    print("Successfully imported Spark Modules")
except ImportError as e:
    print("Can not import Spark Modules", e)


spark = SparkSession.builder.appName('recommend').getOrCreate()

df = spark.read.csv('/Users/Ramin/Desktop/DataPull/movielens_ratings.csv', inferSchema=True, header=True)

df.show()

df.describe().show()

train_data, test_data = df.randomSplit([0.8, 0.2])

als = ALS(maxIter=5, regParam=0.01, userCol='userID', itemCol='movieId', ratingCol='rating')

model = als.fit(train_data)

predictions = model.transform(test_data)

predictions.show()




