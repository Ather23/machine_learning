from pyspark.mllib.linalg import Vectors
from pyspark.sql import Row

from kmeans.kmeans import KmeansAlgo
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import json
import os.path


def init(load_model):
    print("-- Starting--")
    config = get_kmeas_config()
    DATA_FILE_PATH = config["data"]['file_path']
    MODEL_PATH = config["model_path"]
    conf = read_conf()
    sc = SparkContext(conf=conf)


    raw_data = read_data_file(sc, ",", DATA_FILE_PATH)
    sql_context = SQLContext(sc)
    data_schema = sql_context.createDataFrame(raw_data)
    data_schema.registerTempTable("data")

    data_vector = raw_data.map(lambda x: Vectors.dense(x).norm(2)*Vectors.dense(x))

    kmeans_helper = KmeansAlgo()
    kmeans_model = None
    if not load_model:
        kmeans_model = kmeans_helper.kmeans_train(data_vector, [3,4,5])
        kmeans_model.save(sc, MODEL_PATH)
    else:
        kmeans_model = kmeans_helper.kmeans_model_load(sc, MODEL_PATH)


def get_kmeas_config():

    if os.path.isfile('kmeans_config.json') is not True:
        FileNotFoundError
    with open('kmeans_config.json') as data_file:
        config = json.load(data_file)
        return config

def read_data_file(sp_context, delimiter, file_path):
    """
    Parses a csv file and returns a Row object
    """

    raw_data = sp_context.textFile(file_path).cache()
    csv_data = raw_data.map(lambda line: line.split(delimiter)).cache()
    header = csv_data.first()  # extract header
    raw_data = csv_data.filter(lambda x: x != header)
    print(raw_data.take(5))
    row_data = raw_data.map(lambda p: Row(
        number_of_orders=p[0],
        total_order_value=p[1],
        weeks_ago=p[2]
    ))

    return row_data


def read_conf():
    """
    Setting up spark contexts
    """
    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Testing")
    return conf


if __name__ == '__main__':
    global DATA_FILE_PATH
    global MODEL_PATH
    init(False)
