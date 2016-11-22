##  Machine learning as a service with PySpark##

This project provides a general framework to build machine learning as a service using pypspark and flask.

To run this as a service please edit the following file paths:

## **rest_app.py**
```python

if __name__ == "__main__":
    sp_ctx = init_spark_context()
    sys.path.append('/path/to/kmeans folder')
    config = get_kmeas_config()
    MODEL_PATH = config["model_path"]
    create_app(sp_ctx, MODEL_PATH)
```

```python
def init_spark_context():
    conf = SparkConf().setAppName("kmeans-server")
    global sc
    sc = SparkContext(conf=conf, pyFiles=[
        'Path/to/kmeans_engine.py',
        'rest_app.py',
    ])
    return sc
```

##**kmeans_config.json**
```python

{
  "data" : {
    "file_path": "/path/to/data source"
  },

  "model_path" : "/path/to/kmeans model"
}
```

## **Use the following command to run flask on top of spark. Make sure to adust the path to rest_app.py **
```
./spark-submit --master local[*] --total-executor-cores 4 --executor-memory 6g /path/to/rest_app.py
```
