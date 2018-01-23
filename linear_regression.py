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
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.ml.linalg import Vectors
    from pyspark.ml.feature import VectorAssembler, VectorIndexer, OneHotEncoder, StringIndexer
    from pyspark.ml import Pipeline
    print("Successfully imported Spark Modules")
except ImportError as e:
    print("Can not import Spark Modules", e)


spark = SparkSession.builder.appName('lrex').getOrCreate()

df = spark.read.csv('/Users/Ramin/Desktop/DataPull/boc-currency-exchange-rate.csv', inferSchema=True, header=True)


data = df.withColumn("WeekOfYear", weekofyear(df["Date"]))

data2 = data.withColumn("dayOfMonth", dayofmonth(data["Date"]))

data2.printSchema()

good_data = data2.select(["WeekOfYear", "dayOfMonth", "Currency Code", "Rate"])

currency_indexer = StringIndexer(inputCol='Currency Code', outputCol='CurrencyIndex')


model = currency_indexer.fit(good_data)
indexed = model.transform(good_data)

print("Transformed string column '%s' to indexed column '%s'"
      % (currency_indexer.getInputCol(), currency_indexer.getOutputCol()))
indexed.show()


encoder = OneHotEncoder(inputCol="CurrencyIndex", outputCol="CurrencyVec")
encoded = encoder.transform(indexed)
encoded.show()

assembler = VectorAssembler(inputCols=['WeekOfYear', 'dayOfMonth', 'CurrencyVec'],
                            outputCol='features')


output = assembler.transform(encoded)
print("Assembled columns 'WeekOfYear', 'dayOfMonth', 'CurrencyVec' to vector column 'features'")
#output.select("features", "Rate").show(truncate=False)
output.show()

input_model = output.select("features", "Rate")

input_model.show()

train_data, test_data = input_model.randomSplit([0.7,0.3])


linear_regression_boc = LinearRegression(labelCol='Rate')

lrModel = linear_regression_boc.fit(train_data)

print("Coefficients: {} Intercept: {}".format(lrModel.coefficients, lrModel.intercept))

test_results = lrModel.evaluate(test_data)

test_results.residuals.show()

unlabeled_data = test_data.select('features')

predictions = lrModel.transform(unlabeled_data)

predictions.show()


print("RMSE: {}".format(test_results.rootMeanSquaredError))
print("MSE: {}".format(test_results.meanSquaredError))
