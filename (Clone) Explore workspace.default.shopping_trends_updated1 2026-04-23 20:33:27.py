# Databricks notebook source
# DBTITLE 1,Cell 1
import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.offline as pyo
import plotly.graph_objs as go

# COMMAND ----------

#load dataset from the table using spark
df=spark.table('shopping_trends_updated1')
trends=df.toPandas

# COMMAND ----------

# DBTITLE 1,Cell 3
#uderstanding the data
trends = df.toPandas()
trends.info()

# COMMAND ----------

#describing the table
trends.describe

# COMMAND ----------

#list of colums
trends.columns


# COMMAND ----------

#Disply the customer table
display(trends)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# initialize plotly for offline use in notbooks
pyo.init_notebook_mode(connected=True)
df.head()
plt.hist(trends['Age'])

# COMMAND ----------

# DBTITLE 1,Cell 9
#ploting the graph of customer ID and age 
ply = trends.plot(kind='bar', x='Customer ID', y='Age')
plt.show()
plt.figure(figsize=(10,6))
sns.countplot(x='Age', data=trends)

# COMMAND ----------

# DBTITLE 1,Cell 10
#plot dashboard of the customer age, customer category, customer gender
plt.pie(trends['Age'], labels=trends['Category'], autopct='%1.1f%%')
plt.show()
plt.figure(figsize=(10,6))
sns.countplot(x='Gender', data=trends)
