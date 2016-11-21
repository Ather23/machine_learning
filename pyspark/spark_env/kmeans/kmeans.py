"""
A wrapper around spark kmeans.
"""

import itertools
from pyspark.mllib.clustering import KMeans, KMeansModel
from math import sqrt


class KmeansAlgo:

    def kmeans_model_save(self, sc, model,path):
        model.save(sc, path)

    def kmeans_model_load(self, sc,path):
        return KMeansModel.load(sc, path)

    def kmeans_train(self, data_rdd, n_clusters):
        """
        This method is used to train the model
        """

        data_splits = data_rdd.randomSplit([.50, .25, .25], seed=0)
        training_set = data_splits[0].repartition(numPartitions=4).cache()
        validation_set = data_splits[1].repartition(numPartitions=4).cache()
        test_set = data_splits[2].repartition(numPartitions=4).cache()
        max_iter_arr = [50,60,80]
        max_runs =  [50,60,80]
        k_list = n_clusters
        best_model = None
        best_rmse = float("inf")
        best_run = 0
        best_k = 0
        itertools.product
        for itera, run, k in itertools.product(max_iter_arr, max_runs, k_list):
            try:
                model = KMeans.train(training_set, 3, itera, run, "random")
                validation_rmse = model.computeCost(validation_set)
                print("#of clusters k %d\n" % (k))

                if validation_rmse < best_rmse:
                    best_model = model
                    best_rmse = validation_rmse
                    best_run = max_runs
                    best_iter = itera
                    best_k = k
            except Exception as e:
                print(e)
                continue

        # test_preds = best_model.predict(test_set.first())
        print("K-means results...")
        print(str(best_rmse))
        print(str(best_k))

        return best_model

    def error(self, clusters, point):
        center = clusters.centers[clusters.predict(point)]
        return sqrt(sum([x ** 2 for x in (point - center)]))
