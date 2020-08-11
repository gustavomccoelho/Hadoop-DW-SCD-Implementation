from pyspark.context import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit
import pandas as pd

# Retrieves names from tables

def get_table_names(db):

	temp = sqlContext.sql("show tables from " + db).collect()
	table_names = []

	for i in range(len(temp)):
		table_names.append(temp[i][1])
	return table_names

if __name__ == "__main__":

	conf = SparkConf().setAppName('SCD-Implementation')
	sc = SparkContext(conf=conf).getOrCreate()
	sqlContext = HiveContext(sc)

	temp_table_names = get_table_names("temp_adventureworks")
	target_table_names = get_table_names("adventureworks")

	#	If the temp table does not exist on target, it is created

	for i in temp_table_names:

		if i not in target_table_names:

			table = sqlContext.sql("select * from temp_adventureworks." + str(i))
			col_names = sqlContext.sql("describe temp_adventureworks." + str(i)).toPandas()["col_name"]
			primary = ""

			for j in col_names:

				if "id" in j and j != "rowguid":

					primary = j
					break		

			if primary != "":

				unique_count = len(table.toPandas()[primary].unique())
				rows_count = table.count()

				if unique_count != rows_count and "modifieddate" in table.schema.names:

					table2 = table.sort(primary, "modifieddate")
					duplicated = table2.toPandas().duplicated(primary, "first")
					table3 = table2.toPandas()
					table3["Version"] = duplicated.apply(lambda x: 0 if x == False else 1)
					table3 = sqlContext.createDataFrame(table3)
					table3.createOrReplaceTempView("tempTable")
					sqlContext.sql("create table adventureworks." + str(i) + " as select * from tempTable")

