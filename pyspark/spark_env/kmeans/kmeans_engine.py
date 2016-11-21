import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from pyspark.mllib.clustering import KMeansModel
import numpy as np


class KmeansEngine:
    model = None

    def get_predictions(self, vector):
        """
        Returns the predicted cluster of a vector
        """

        if self.model is None : ValueError
        clusters = self.model.clusterCenters
        index = self.model.predict(vector)
        result = dict()
        result["index"] = index
        result["cluster"] = np.array_str(np.array(clusters[index]))
        return result

    @property
    def get_cluster_centers(self):
        """
        Returns cluster centers
        """

        logger.info("Fetching clusters...")
        if self.model is None: ValueError
        centers = self.model.clusterCenters
        result_centers = dict()
        i = 0
        for c in centers:
            result_centers[i] = np.array_str(c)
            i += 1
        return result_centers

    def __init__(self, spark_contect, model_path):
        self.model = KMeansModel.load(spark_contect, model_path)
