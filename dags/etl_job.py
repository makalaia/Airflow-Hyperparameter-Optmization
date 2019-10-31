import numpy as np
import pandas as pd
import pyspark.sql.functions as Spark_F
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, StructType, StructField, IntegerType


def _make_var_cyclic(series):
    uniques = np.sort(np.unique(series))
    diff = np.arange(0, 2 * np.pi, 2 * np.pi / len(uniques))
    cos, sin = np.cos(diff), np.sin(diff)

    x_dict = {k: v for (k, v) in zip(uniques, cos)}
    y_dict = {k: v for (k, v) in zip(uniques, sin)}

    if isinstance(series, pd.Series):
        return series.apply(lambda x: x_dict[x]), series.apply(lambda x: y_dict[x])

    return np.array([x_dict[i] for i in series]), np.array([y_dict[i] for i in series])


schema = StructType([
  StructField("year", IntegerType()),
  StructField("month_x", DoubleType()),
  StructField("month_y", DoubleType()),
  StructField("weekday_x", DoubleType()),
  StructField("weekday_y", DoubleType()),
  StructField("day_x", DoubleType()),
  StructField("day_y", DoubleType()),
  StructField("hour_x", DoubleType()),
  StructField("hour_y", DoubleType()),
  StructField("traffic_volume", DoubleType())
])
# FIXME: PandasUDFType.SCALAR will work with StructType in spark 3.0
@pandas_udf(returnType=schema, functionType=PandasUDFType.GROUPED_MAP)
def parse_date(df_date):
    series = df_date['date_time']

    df = pd.DataFrame()
    df['year'] = series.apply(lambda x: x.year)
    df['month_x'], df['month_y'] = _make_var_cyclic(series.apply(lambda x: x.month))
    df['weekday_x'], df['weekday_y'] = _make_var_cyclic(series.apply(lambda x: x.dayofweek))
    df['day_x'], df['day_y'] = _make_var_cyclic(series.apply(lambda x: x.day))
    df['hour_x'], df['hour_y'] = _make_var_cyclic(series.apply(lambda x: x.hour))
    df['traffic_volume'] = df_date['traffic_volume']

    return df


def normalize_data(df, columns=None):
    selectExpr = list()
    if columns is None:
        columns = df.columns
    for column in columns:
        average = df.agg(Spark_F.mean(df[column]).alias("mean")).collect()[0]["mean"]
        std = df.agg(Spark_F.stddev(df[column]).alias("stddev")).collect()[0]["stddev"]
        selectExpr.append(((df[column] - average) / std).alias(column))
    return df.select(selectExpr)


def etl_method(conf, test_size=.1):
    spark = SparkSession.builder.appName('load').config(conf=conf).getOrCreate()
    GROUP_COLUMN = 'id'

    print('LOADING DATA')
    df = spark.read.csv('s3a://test/data/RAW_DATA_Metro_Interstate_Traffic_Volume.csv.gz', header=True, inferSchema=True)
    df = df[['date_time', 'traffic_volume']]
    df = df.withColumn(GROUP_COLUMN, Spark_F.lit(0))

    print('PREPROCESSING DATA')
    # parse date
    data = df.groupby(GROUP_COLUMN).apply(parse_date)

    # scale data
    data = normalize_data(data)

    # train-test split
    size = data.count()
    train_size = round((1 - test_size) * size)
    test_size = size - train_size
    data_train = data.limit(train_size)

    data_path = 's3a://test/data/train_preprocessed.parquet'

    data_train.repartition(2).write.mode('overwrite').parquet(data_path)

    return data_path


if __name__ == '__main__':
    conf = SparkConf()
    conf.set("spark.hadoop.fs.s3a.endpoint", 'http://localstack:4572')
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
    conf.set("spark.hadoop.fs.s3a.access.key", "XXX")
    conf.set("spark.hadoop.fs.s3a.secret.key", "XXX")

    etl_method(conf)
