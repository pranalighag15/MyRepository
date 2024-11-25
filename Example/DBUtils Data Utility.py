# Databricks notebook source
dbutils.data.help

# COMMAND ----------

df=spark.read.options(header=True).csv("dbfs:/FileStore/tables/Pranali_18Sep/orders.csv") 

df.display()

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Pranali_18Sep/

# COMMAND ----------

dbutils.notebook.run('/Workspace/Users/pranali.prakash.ghag@accenture.com/SumitMitalDemo/MyFirstNotebook',60)

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.help("combobox")

# COMMAND ----------

dbutils.widgets.combobox(name='orderstatus', label='Order Status', defaultValue='CLOSED', choices=['CLOSED', 'COMPLETE','PENDING_PAYMENT','PROCESSING'])

# COMMAND ----------

df=spark.read.options(header=True).csv("dbfs:/FileStore/tables/Pranali_18Sep/orders.csv")

newwidget=dbutils.widgets.get('orderstatus')

df.where(df.order_status==newwidget).show()


# COMMAND ----------

df_widget=spark.read.options(header=True).csv("dbfs:/FileStore/tables/Pranali_18Sep/orders.csv")

dbutils.widgets.dropdown(name='orderstatusdrop', defaultValue='CLOSED', choices=['CLOSED', 'COMPLETE','PENDING_PAYMENT','PROCESSING'])

df_new=dbutils.widgets.get('orderstatusdrop')

df_widget.where(df_widget.order_status==df_new).show()

# COMMAND ----------


