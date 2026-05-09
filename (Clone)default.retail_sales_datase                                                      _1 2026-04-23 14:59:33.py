# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')
import datetime as dt

# COMMAND ----------

#load data from the table using spark
df=spark.table('retail_sales_dataset')
retail_df = df.toPandas()




# COMMAND ----------

#understanding the data
retail_df.info()


# COMMAND ----------

#describing the data table
retail_df.describe()



# COMMAND ----------

#list of columns
retail_df.columns


# COMMAND ----------

display(retail_df)









# COMMAND ----------

#the summary of the data
print(retail_df)

# COMMAND ----------

#indicate the first 5 rows within the data
retail_df.head(5)

# COMMAND ----------

#indicate the last 5 rows within the data
retail_df.tail(5)



# COMMAND ----------

#checking for duplicated values in our table/data
retail_df.duplicated().sum()

# COMMAND ----------

#checking for nall values in our table/data
retail_df.isnull().sum()


# COMMAND ----------

#dropna removes rows that have missing values
retail=retail_df.dropna()

# COMMAND ----------


retail.dtypes

# COMMAND ----------

#it replaces missing values 
retail=retail_df.fillna(0)

# COMMAND ----------


retail_df.shape

# COMMAND ----------


pd.DatetimeIndex(retail_df['Date'])

# COMMAND ----------

date_column=pd.DatetimeIndex(retail_df['Date'])
retail_df['year']=date_column.year
retail_df['month']=date_column.month
retail_df['day']=date_column.day

# COMMAND ----------

display(retail_df)

# COMMAND ----------

retail_df.groupby('month')['Total Amount'].sum()

# COMMAND ----------

#sales by category(matplotlib)
sales_by_category=retail_df.groupby('Product Category')['Total Amount'].sum()
plt.figure
sales_by_category.plot.bar()
plt.title('total sales by category')
plt.xlabel('category')
plt.ylabel('total sales')
plt.show()

# COMMAND ----------

# DBTITLE 1,Cell 21
#Sales Trend Overtime
retail_df['Date'] = pd.to_datetime(retail_df['Date'])
salestrend = retail_df.groupby('Date')['Total Amount'].sum()
plt.figure()
salestrend.plot()
plt.xlabel('Date')
plt.ylabel('Total Sales')
plt.show()

# COMMAND ----------

# DBTITLE 1,Cell 22
# seaborn sales by region
import matplotlib.pyplot as plt
plt.figure()
sns.barplot(data=retail_df, x='Product Category', y='Total Amount')
plt.title('Sales by Category')
plt.show()
