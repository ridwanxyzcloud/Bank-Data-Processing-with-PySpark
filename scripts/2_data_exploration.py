#data investigation
bank_df.show()

# schema investigation
bank_df.printSchema()

# row count, data count
bank_df.count()

# number of column 
len(bank_df.columns)

 #checking for null values
for column in bank_df.columns:
    print(column, 'Nulls', bank_df.filter(bank_df[column].isNull()).count())

# Fillna in respect to data type
bank_df_clean = bank_df.fillna({
    'Customer_Name':'Unknown',
    'Customer_Address' : 'Unknown',
    'Customer_City':'Unknown',
    'Customer_State':'Unknown',
    'Customer_Country':'Unknown',
    'Company':'Unknown',
    'Job_Title':'Unknown',
    'Email':'Unknown',
    'Phone_Number':'Unknown',
    'Credit_Card_Number':0,
    'IBAN':'Unknown',
    'Currency_Code':'Unknown',
    'Random_Number':0.0,
    'Category':'Unknown',
    'Group':'Unknown',
    'Is_Active':'Unknown',
    'Description':'Unknown',
    'Gender':'Unknown',
    'Marital_Status':'Unknown'
})

#drop 'Last_Updated' column . it is about 10% of the whole data
bank_df_clean = bank_df_clean.dropna(subset=['Last_Updated'])

# check null value to verify changes
bank_df_clean.filter(col("Last_Updated").isNull()).show()

