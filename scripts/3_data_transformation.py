# transaction table 
transaction_dim = bank_df_clean.select('Transaction_Date','Amount','Transaction_Type')

# adding transaction_id column using monotonycally increasing id function
transaction_dim = transaction_dim.withColumn('transaction_id', monotonically_increasing_id())

# re-arrange 
transaction_dim = transaction_dim.select('transaction_id','Amount','Transaction_Type','Transaction_Date')

# customer table
# adding distinct when pulling the data optimise the table by reducing redundancy

customer_dim = bank_df_clean.select('Customer_Name','Customer_Address','Customer_City','Customer_State','Customer_Country').distinct()
customer_dim = customer_dim.withColumn('customer_id', monotonically_increasing_id())
customer_dim = customer_dim.select('customer_id','Customer_Name','Customer_Address','Customer_City','Customer_State','Customer_Country')

# employee table

employee_dim = bank_df_clean.select('Company','Job_Title', 'Gender','Marital_Status').distinct()
employee_dim = employee_dim.withColumn('employee_id', monotonically_increasing_id())
employee_dim = employee_dim.select('employee_id','Job_Title','Company','Gender','Marital_Status')

# bank_df_fact table 

bank_df_fact = bank_df_clean.join(customer_dim, ['Customer_Name','Customer_Address','Customer_City','Customer_State','Customer_Country'], 'left') \
                            .join(transaction_dim, ['Amount','Transaction_Type','Transaction_Date'], 'left') \
                            .join(employee_dim, ['Job_Title', 'Gender','Marital_Status'], 'left')\
                            .select('transaction_id','customer_id','employee_id','Credit_Card_Number','IBAN','Currency_Code','Random_Number','Category','Group','Is_Active','Last_Updated','Description')
