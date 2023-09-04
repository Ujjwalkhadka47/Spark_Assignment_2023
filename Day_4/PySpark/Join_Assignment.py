
# %% [markdown]
# ## Joins

# %%
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("day3").getOrCreate()


# %% [markdown]
# 
# Question: You are given two DataFrames: employees_df and departments_df, which contain information about employees and their respective departments. The schema for the DataFrames is as follows:
# 
# employees_df schema:
# |-- employee_id: integer (nullable = true)
# |-- employee_name: string (nullable = true)
# |-- department_id: integer (nullable = true)
# 
# departments_df schema:
# 
# |-- department_id: integer (nullable = true)
# |-- department_name: string (nullable = true)
# 
# Employees DataFrame:
#                                                                                 
# +-----------+-------------+-------------+
# |employee_id|employee_name|department_id|
# +-----------+-------------+-------------+
# |1          |Pallavi mam  |101          |
# |2          |Bob          |102          |
# |3          |Cathy        |101          |
# |4          |David        |103          |
# |5          |Amrit Sir    |104          |
# |6          |Alice        |null         |
# |7          |Eva          |null         |
# |8          |Frank        |110          |
# |9          |Grace        |109          |
# |10         |Henry        |null         |
# +-----------+-------------+-------------+
# 
# 
# 
# Departments DataFrame:
# +-------------+------------------------+
# |department_id|department_name         |
# +-------------+------------------------+
# |101          |HR                      |
# |102          |Engineering             |
# |103          |Finance                 |
# |104          |Marketing               |
# |105          |Operations              |
# |106          |null                    |
# |107          |Operations              |
# |108          |Production              |
# |null         |Finance                 |
# |110          |Research and Development|
# +-------------+----------------------
# 

# %%
from pyspark.sql.types import StructField,StructType,StringType,IntegerType

employee_data = [(1,'Pallavi mam',101),
                 (2,'Bob',102),
                 (3,'Cathy',101),
                 (4,'David',103),
                 (5,'Amrit Sir',104),
                 (6,'Alice',None),
                 (7,'Eva',None),
                 (8,'Frank',110),
                 (9,'Grace',109),
                 (10,'Henry',None)]

Department_Data = [(101,'Hr'),
                   (102,'Engineering'),
                   (103,'Finance'),
                   (104,'Marketing'),
                   (105,'Operation'),
                   (106,None),
                   (107,'Operations'),
                   (108,'Production'),
                   (None,'Finance'),
                   (110,'Research and Development')]

employee_schema = StructType([
    StructField("Employee_Id",IntegerType(),True),
    StructField("Employee_name",StringType(),True),
    StructField("department_id",IntegerType(),True)
])

department_schema = StructType([
    StructField("department_id",IntegerType(),True),
    StructField("department_name",StringType(),True)
])

employee_df  = spark.createDataFrame(data=employee_data,schema=employee_schema)
department_df = spark.createDataFrame(data=Department_Data, schema=department_schema)

print("Employee Dataframe")
employee_df.printSchema()
employee_df.show()

print("Department Dataframe")
department_df.printSchema()
department_df.show(truncate=False)

# %%
# This was the expected output ~ which matches to the output I generated.

# %% [markdown]
# ### Join Expressions
# 
# Question: How can you combine the employees_df and departments_df DataFrames based on the common "department_id" column to get a combined DataFrame with employee names and their respective department names?

# %%
joinExpression = department_df['department_id'] == employee_df['department_id']

department_df.join(employee_df, joinExpression).select(department_df['department_id'],employee_df['employee_id'],employee_df['employee_name'],department_df['department_name']).show()

# %% [markdown]
# ### Inner Joins
# 
# Question: How can you retrieve employee names and their respective department names for employees belonging to the "Engineering" department?

# %%
innerJoinExpression = department_df['department_id'] == employee_df['department_id']
engineeringExpression = department_df['department_name'] == 'Engineering'

department_df.join(employee_df, innerJoinExpression).select(employee_df['employee_name'], department_df['department_name']).where(engineeringExpression).show()

# %% [markdown]
# ### Outer Joins
# 
# Question: Retrieve a DataFrame that contains all employees along with their department names. If an employee doesn't have a department assigned, display "No Department".

# %%

joinType = 'outer'

#Outer joined two dataframes which returned every value possible and null if any value is not present
outer_joined_df = department_df.join(employee_df, joinExpression, joinType).select(employee_df['employee_name'],department_df['department_name'])

#filled null with default value as No Department
No_dept_filled = outer_joined_df.na.fill('No Department')
No_dept_filled.show()

# %% [markdown]
# ### Left Outer Joins
# 
# Question: List all employees along with their department names. If an employee doesn't have a department assigned, display "No Department".

# %%
joinType = 'left_outer'
employee_df.join(department_df,joinExpression,joinType).select(employee_df['employee_name'],department_df['department_name']).na.fill('no Department').show()

# %% [markdown]
# ### Right Outer Joins
# 
# Question: Display a list of departments along with employee names. If a department has no employees, display "No Employees".
# 
# 

# %%
"""
    I guess there is problem in the given expected output.
    The expected output also gives the null value in the department column as "No Employee", but i guess No employee should only be given
    in employee column so did so.

    for the result to be as given in assignment we can remove subset=[] from fill()
"""

joinType = 'right_outer'

employee_df.join(department_df,joinExpression,joinType).select(department_df['department_name'],employee_df['employee_name']).na.fill('No Employee', subset=['employee_name']).show()

# %% [markdown]
# ### Left Semi Joins
# 
# Question: Retrieve a DataFrame that includes employee names for departments that have employees.
# 
# 

# %%
joinType = 'left_semi'

employee_df.join(department_df,joinExpression,joinType).select(employee_df['employee_name']).show()


# %% [markdown]
# ### Left Anti Joins
# 
# Question: Find the employees who don't belong to any department.

# %%
joinType = 'left_anti'

employee_df.join(department_df,joinExpression,joinType).select(employee_df['employee_name']).show()

# %% [markdown]
# ### Cross (Cartesian) Joins
# 
# Question: Create a DataFrame that contains all possible combinations of employees and departments.

# %%
joinType = 'cross'
employee_df.crossJoin(department_df).show()
