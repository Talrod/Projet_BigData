#Importation des bibliothèques et des modules

import numpy as np
import math
from random import random
from pyspark import SparkContext, SparkConf 
from pyspark.sql import SparkSession
from operator import add
from time import time


#Initialisation de la session Spark
spark = SparkSession.builder.appName('EstimationPi').getOrCreate()
sc = spark.sparkContext


#Fonction simulant un point p avec deux coordonnées x et y et determine si ce dernier se situe à l'intérieur ou à l'extérieur du cercle.
def is_point_inside_unit_circle(p):
    x, y = random(), random()
    if x*x + y*y < 1:
      return 1
    else: 
      return 0


#Fonction permettant d'estimer pi en utilisant Spark. Il s'agira d'appliquer n fois la fonction précédente et compter le nombre de points se trouvant dans le cercle.
def pi_estimator_spark(n):
    count = sc.parallelize(range(0, n)).map(is_point_inside_unit_circle).reduce(add)
    return 4 * count/n
        
#Fonction permettant d'estimer pi en utilisant Numpy.
def pi_estimator_numpy(n):  
    count = 0
    for i in range(n):
        a = is_point_inside_unit_circle()
        if a == 1:
            count += 1
    return 4*count/n

#Initialisation de n
n = 100000

#Execution de la fonction pour calculer Pi avec Spark et mesure du temps d'éxécution.
startTime = time() #Temps au début
estimationPi_Spark = pi_estimator_spark(n) 
print("Temps d'execution avec Spark :", time()-startTime)
print("Pi est environ egale a %f" % estimationPi_Spark)

#Execution de la fonction pour calculer Pi avec Numpy et mesure du temps d'éxécution.
startTime = time() #Temps au début
estimationPi_Numpy = pi_estimator_numpy(n) 
print("Temps d'execution avec Numpy :", time()-startTime)
print("Pi est environ egale a %f" % estimationPi_Numpy)
