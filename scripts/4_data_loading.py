# outputing cleaned and transformed data to parquet
# partitioning will optimise query 
bank_df_fact.write.mode('overwrite').partitionBy('transaction_id').parquet(r'data/processed/bank_df_fact')
transaction_dim.write.mode('overwrite').parquet(r'data/processed/transaction_dim')
employee_dim.write.mode('overwrite').parquet(r'data/processed/employee_dim')
customer_dim.write.mode('overwrite').parquet(r'data/processed/customer_dim')

# outputing to a csv file 
# repartitioning 

bank_df_fact.repartition(3)write.mode('overwrite').option('header', 'true').csv(r'data/processed/bank_df_fact')
transaction_dim.repartition(3)write.mode('overwrite').option('header', 'true').csv(r'data/processed/transaction_dim')
employee_dim.repartition(3)write.mode('overwrite').option('header', 'true').csv(r'data/processed/employee_dim')
customer_dim.repartition(3)write.mode('overwrite').option('header', 'true').csv(r'data/processed/customer_dim')


# Loading to database
# Retrieve Azure SQL Database credentials from environment variables
sql_user = os.getenv("az_user")
sql_password = os.getenv("az_password")

# Azure SQL Database connection properties
sql_url = "jdbc:sqlserver://ridwanclouds.database.windows.net:1433;database=bankdb"
sql_properties = {
    "user": sql_user,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Write the DataFrame to Azure SQL Database
bank_df_fact_df.write.mode("overwrite").jdbc(url=sql_url, table="dbo.bank_df_fact", properties=sql_properties)
transaction_dim.write.mode("overwrite").jdbc(url=sql_url, table="dbo.transaction_dim", properties=sql_properties)
customer_dim.write.mode("overwrite").jdbc(url=sql_url, table="dbo.customer_dim", properties=sql_properties)
employee_dim.write.mode("overwrite").jdbc(url=sql_url, table="dbo.employee_dim", properties=sql_properties)