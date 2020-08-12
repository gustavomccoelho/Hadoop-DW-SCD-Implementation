# What are Slowly Changing Dimensions (SCD)?

SCDs are dimensions in data management and data warehousing containing relatively static data about such entities as geographical locations, customers, or products. Data captured by Slowly Changing Dimensions (SCDs) change slowly but unpredictably, rather than according to a regular schedule.

Dealing with these issues involves SCD management methodologies referred to as Type 0 through 6. On this case, we will consider type number 2.

# SCD Type 2

This method tracks historical data by creating multiple records for a given natural key in the dimensional tables with separate surrogate keys and/or different version numbers. Unlimited history is preserved for each insert. For example, if the supplier relocates to Illinois the version numbers will be incremented sequentially:

![type2](/pictures/type2.PNG)

# Background

This is an exercise proposed by Data Science Academy at the end of the course related to Data Engineering with Hadoop. The purpose is to collect given data from a MySQL database and proceed with the SCD creation by using the Hadoop enviroment. 

# Dataset

The provided dataset is composed by several tables, as below:

![tables](/pictures/tables.PNG)

Let's see the table employeepayhistory as an example:

![employeepayhistory](/pictures/employeepayhistory.PNG)

# Step-by-step

Step 1: the database is transfered from MySQL to a Data Warehouse (DW) in Hive, by using sqoop commands. Sqoop is a fairly simple, friendly and usefull tool, but it does not always allow generalization on import commands. On this case, specific commands are made to particular tables, e.g. allowing string columns as Primary Key, setting specific column format, etc. For this reason, the sqoop commands were made individually and placed together on a single .sh file.   

```
bash sql_to_hive.sh
```

Step 2: With the DW in place, it is time to use Spark to create our SCDs. This is done by runing the following scrip:

```
spark-submit SCD.py
```

# Result

Now we should have our tables with SCDs available. Using our previous example, this is how employeepayhistory looks like at the end:

![employeepayhistorySCD](/pictures/employeepayhistorySCD.PNG)
