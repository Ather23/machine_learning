from sqlite3 import Row

from pyspark import SparkConf
from pyspark import SparkContext


DATA_FILE_PATH = ""

def init():
    print("-- Starting--")
    conf = read_conf()
    sc = SparkContext(conf=conf)


def read_data_file(sp_context, delimiter, file_path):
    """
    Parses a csv file and returns a Row object

    """

    raw_data = sp_context.textFile(file_path).cache()
    csv_data = raw_data.map(lambda line: line.split(delimiter)).cache()
    header = csv_data.first()  # extract header
    raw_data = csv_data.filter(lambda x: x != header)
    '''
    TODO:
    '''
    row_data = raw_data.map(lambda p: Row(
        # sku=p[0],
        # product_price=p[1],
        # sales_rank=p[2],
        # reviews=p[3],
        # image_size=p[4],
        # returns=p[5],
        # cancels=p[6],
        # stock_status=p[7],
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
    init()
