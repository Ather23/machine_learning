import os
import sys
import json
import logging
from kmeans.kmeans_engine import KmeansEngine
from pyspark import SparkContext, SparkConf
from flask import Flask, request, render_template

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


TEMPLATES = 'path/to/index.html'

app = Flask(__name__, template_folder=TEMPLATES)


@app.route("/", methods=["GET"])
def index():
    return render_template('index.html')


@app.route("/centers", methods=["GET"])
def get_centers():
    centers = kmeans_engine.get_cluster_centers
    return json.dumps(centers)


@app.route("/predict", methods=["POST"])
def get_predictions():
    content = request.get_json()
    if content["vector"] is None: ValueError
    request_v = content["vector"]
    v = []
    for e in request_v.split(','):
        v.append(float(e))
    predictions = kmeans_engine.get_predictions(v)
    return json.dumps(predictions)

def create_app(spark_context, model_path):
    global kmeans_engine
    kmeans_engine = KmeansEngine(spark_context, model_path)
    app.run(host='0.0.0.0')


def init_spark_context():
    conf = SparkConf().setAppName("kmeans-server")
    global sc
    sc = SparkContext(conf=conf, pyFiles=[
        'Path/to/kmeans_engine.py',
        'rest_app.py',
    ])
    return sc


def get_kmeas_config():

    if os.path.isfile('kmeans_config.json') is not True:
        FileNotFoundError
    with open('kmeans_config.json') as data_file:
        config = json.load(data_file)
        return config

if __name__ == "__main__":
    sp_ctx = init_spark_context()
    sys.path.append('/kmeans')
    config = get_kmeas_config()
    MODEL_PATH = config["model_path"]
    create_app(sp_ctx, MODEL_PATH)
