# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b4fa02c7-a29c-4fa1-ae6c-ba6ec7f5889a",
# META       "default_lakehouse_name": "Destination_LK",
# META       "default_lakehouse_workspace_id": "a5f2dec7-f9e7-4002-98a8-8f8e5f452bcc"
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data=[
    [1,'credit card transactions','credit_card_transactions_ibm_v2','sales','credit_card_transactions'],
    [2,'credit card transactions','sd254_cards','sales','sd254_cards'],
    [3,'credit card transactions','sd254_users','sales','sd254_users'],
    [4,'credit card transactions','User0_credit_card_transactions','sales','User0_credit_card_transactions']
]
columns=['id', 'file_directory','S_file_name','schema_name','D_filename']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DF=spark.createDataFrame(data,columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(DF)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DF.write.mode('overwrite').saveAsTable('dbo.tableslist')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
