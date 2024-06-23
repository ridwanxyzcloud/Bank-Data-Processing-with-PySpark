# import necessary dependencies
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import pandas as pd

# initializing a spark session, and check session
spark = SparkSession.builder.appName("BankTransactionETL").getOrCreate()
spark

