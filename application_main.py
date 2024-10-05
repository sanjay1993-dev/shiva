import sys 
from lib import Datamanipulation,Datareader,Utils 
from pyspark.sql.functions import * 
if __name__  ==  '__main__':
 
  if len(sys.argv) < 2:

    print("Please specify the environment")
    sys.exit(-1)

  job_run_env = sys.argv[1]

  print("CreatingSparkSession")

  spark=Utils.get_spark_session(job_run_env)

  print("CreatedSparkSession")

  orders_df=Datareader.read_orders(spark,job_run_env)

  orders_filtered=Datamanipulation.filter_closed_orders(orders_df)

  customers_df=Datareader.read_customers(spark,job_run_env)

  joined_df=Datamanipulation.join_orders_customers(orders_filtered,customers_df)

  aggregated_results=Datamanipulation.count_orders_state(joined_df)

  aggregated_results.show()

  print("endofmain")
  print("I am happy")
  dgadhksdhksahdksah