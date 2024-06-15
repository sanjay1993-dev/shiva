import configparser
from pyspark import SparkConf
#loading application configs in python dictionary
def get_app_config(env):
    config = configparser.ConfigParser()
    config.read("configs/application.conf")
    app_conf={}

    for (key,val) in config.items(env):
     app_conf[key]=val
     return app_conf
#loading the pyspark configs and creating spark conf object
def get_pyspark_config(env):
   config=configparser.ConfigParser()
   config.read("configs/pyspark.conf")

   conf = SparkConf()
   for (key,val) in config.items(env):
     conf.set(key,val)
   return conf   