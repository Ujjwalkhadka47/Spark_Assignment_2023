from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("day4").getOrCreate()



# %%
# Load the Chipotle dataset into a Spark DataFrame
data_path = "/Users/anujkhadka/Fusemachines47/ALL SPARK/Spark_Assignment_2023/Day_4/US_Crime_Rates_1960_2014 (1).csv"  # Replace with the actual path
US_Crime_Rates_1960_2014_df = spark.read.csv(data_path, header=True, inferSchema=True)

# Load the Chipotle dataset into a Spark DataFrame
data_path = "/Users/anujkhadka/Fusemachines47/ALL SPARK/Spark_Assignment_2023/Day_4/titanic.csv"  # Replace with the actual path
titanic_df = spark.read.csv(data_path, header=True, inferSchema=True)


# %%
US_Crime_Rates_1960_2014_df.printSchema()

# %%
US_Crime_Rates_1960_2014_df.show()

# %% [markdown]
# ### count
# 
# Question: How many records are there in the US_Crime_Rates_1960_2014_df DataFrame?

# %%
# counting the records in US crime dataframe

print(f"Total number of records: {US_Crime_Rates_1960_2014_df.count()}")

# %% [markdown]
# ### countDistinct
# Question: How many distinct years are present in the US_Crime_Rates_1960_2014_df DataFrame?
# Answer:

# %%
from pyspark.sql.functions import countDistinct

year_count = US_Crime_Rates_1960_2014_df.select(countDistinct('year'))   # this returns a dataframe with count value
# year_count.show()

print(f"Number of distinct year is: {year_count.collect()[0][0]}")    #Used collect to get the value i.e get the value of the 0th row and 0th column -- 55

# %% [markdown]
# ### approx_count_distinct
# 
# Question: Estimate the approximate number of distinct values in the "Total" column of the US_Crime_Rates_1960_2014_df DataFrame.

# %%
from pyspark.sql.functions import approx_count_distinct
total_count = US_Crime_Rates_1960_2014_df.select(approx_count_distinct('total'))  #Returns the approximate count is a dataframe
# total_count.show()

print(f"Number of approx distinct count of total is: {total_count.collect()[0][0]}")  # I extraxted the value from the dataframe as the count

# %% [markdown]
# ###  first and last
# 
# Question: Find the first and last year in the US_Crime_Rates_1960_2014_df DataFrame.

# %%
from pyspark.sql.functions import first,last

first_last = US_Crime_Rates_1960_2014_df.select(first('year'),last('year'))
first_last.show()


#This is for generating the result as expected in the assignment
print(f"First Year: {first_last.collect()[0][0]}")
print(f"Last Year: {first_last.collect()[0][1]}")

# %% [markdown]
# ### min and max
# 
# Question: Find the minimum and maximum population values in the US_Crime_Rates_1960_2014_df DataFrame.

# %%


# %%
from pyspark.sql.functions import max,min

max_min = US_Crime_Rates_1960_2014_df.select(max('population'),min('population'))
max_min.show()

new_line = '\n'
# To get the result as expected in the assignment.
print(f"Minimum population: {max_min.collect()[0][1]} {new_line}Maximum Population: {max_min.collect()[0][0]}")

# %% [markdown]
# ### sumDistinct
# 
# Question: Calculate the sum of distinct "Property" values for each year in the US_Crime_Rates_1960_2014_df DataFrame.

# %%


# %%
from pyspark.sql.functions import sumDistinct,col

sum_distinct_property_by_year = US_Crime_Rates_1960_2014_df.groupBy("Year").agg(sumDistinct("Property").alias("SumDistinctProperty")).orderBy(col('year'))
sum_distinct_property_by_year.show()

# %% [markdown]
# ### avg
# 
# Question: Calculate the average "Murder" rate for the entire dataset in the US_Crime_Rates_1960_2014_df DataFrame.
# Answer:

# %%
from pyspark.sql.functions import avg

average_murder = US_Crime_Rates_1960_2014_df.select(avg('murder'))
average_murder.show()

print(f"The average murder rate in USA is: {average_murder.collect()[0][0]}")

# %% [markdown]
# ### Aggregating to Complex Types
# 
# Question: Calculate the total sum of "Violent" and "Property" crimes for each year in the US_Crime_Rates_1960_2014_df DataFrame. Store the results in a struct type column.

# %%


# %% [markdown]
# ### Grouping
# 
# Question: In the given US_Crime_Rates_1960_2014_df DataFrame, you are tasked with finding the average of all crimes combined for each year. Calculate the sum of all crime categories (Violent, Property, Murder, Forcible_Rape, Robbery, Aggravated_assault, Burglary, Larceny_Theft, Vehicle_Theft) for each year and then determine the average of these combined crime sums. Provide the result as the average of all crimes across the entire dataset.

# %%


# %%
from pyspark.sql.functions import col,avg

total_sum = US_Crime_Rates_1960_2014_df.withColumn("Total_crime_sum",
                                                    col('violent') + col('property') + col('murder') + col('forcible_rape') + 
                                                    col('robbery') + col('Aggravated_assault') + col('Burglary')+ col('Larceny_Theft')
                                                    + col('Vehicle_Theft')).alias('Total_crime_sum')

# total_sum.show()


avegare_crime = total_sum.agg(avg('total_crime_sum'))
print(f"Average of all crime: {avegare_crime.collect()[0][0]}")

total_sum.select('year','total_crime_sum').show()
# avegare_crime.show()
# average_crime = total_sum.groupBy('year').agg(avg('total_crime_sum').alias('average_crime'))
# average_crime.show()

# %% [markdown]
# ### Window Functions
# 
# Question: Calculate the cumulative sum of "Property" values over the years using a window function in the US_Crime_Rates_1960_2014_df DataFrame.

# %%
# I think the this is not what the output expects.

# %%
from pyspark.sql.window import Window 
from pyspark.sql.functions import sum as _sum 

windowSpec = Window.orderBy('year').rowsBetween(Window.unboundedPreceding, Window.currentRow)

new_df = US_Crime_Rates_1960_2014_df.withColumn('cum_sum', _sum('property').over(windowSpec))
new_df.show()

# %% [markdown]
# ### Pivot
# Question: You are working with a DataFrame named US_Crime_Rates_1960_2014_df that contains crime data for different crime types over the years. 

# %%
#This is the expected output of the question.

# %%
from pyspark.sql.functions import asc,desc


#created a pivot table of year where the value of robbery of the year is shown.
pivot_table_robery = US_Crime_Rates_1960_2014_df.groupBy('year').pivot('year').sum('robbery').sort(asc('year'))
pivot_table_robery.show()
