# Lab 3: Explore Big Data using Azure Databricks
In this lab you will use Azure Databricks to explore the New York Taxi data files you saved in your data lake in Lab 2. Using a Databricks notebook you will connect to the data lake and query taxi ride details. 

The estimated time to complete this lab is: **45 minutes**.

## Lab Architecture
![Lab Architecture](./Media/Lab3-Image01.png)

Step     | Description
-------- | -----
![](./Media/Red1.png) |Build an Azure Databricks notebook to explore the data files you saved in your data lake in the previous exercise. You will use Python and SQL commands to open a connection to your data lake and query data from data files.

**IMPORTANT**: Some of the Azure services provisioned require globally unique name and a “-suffix” has been added to their names to ensure this uniqueness. Please take note of the suffix generated as you will need it for the following resources in this lab:

Name	                     |Type
-----------------------------|--------------------
mdwdatalake*suffix*	         |Storage Account
MDWDatabricks-*suffix*	     |Databricks Workspace
mdwsqlvirtualserver-*suffix* |SQL server

**IMPORTANT**: The code snippets below illustrate the simplest and quickest way to establish connections between Databricks and other Azure services. They **ARE NOT** considered best practices as they expose secrets and passwords in plain text. For a secure implementation following the security best practices, please consider the use of Azure Key Vault in conjuntion with Databricks Secret Scopes (https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html).

## Create Azure Databricks Cluster 
In this section you are going to create an Azure Databricks cluster that will be used to execute notebooks.

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	In the Azure Portal, navigate to the lab resource group and locate the Azure Databricks resource MDWDatabricks-*suffix*.
2.	On the **MDWDatabricks-*suffix*** blade, click the **Launch Workspace** button. The Azure Databricks portal will open on a new browser tab.

    ![](./Media/Lab3-Image02.png)

3.	On the Azure Databricks portal, click the **Clusters** button on the left-hand side menu. 
4.	On the **Clusters** blade, click **+ Create Cluster**.

    ![](./Media/Lab3-Image03.png)

5.	On the **Create Cluster** blade, enter the following connection details:
    <br>- **Cluster Name**: MDWDatabricksCluster
    <br>- **Max Workers**: 4

    Leave all other fields with their default values.

6.	Click **Create Cluster**. It should take around 5 minutes for the cluster to be fully operational.

    ![](./Media/Lab3-Image04.png)

## Create an Azure Databricks Notebook 
In this section you are going to create an Azure Databricks notebook that will be used to explore the taxi data files you copied to your data lake in the Lab 2. 

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	On the Azure Databricks portal, click the **Home** button on the left-hand side menu. 
2.	On the **Workspace** blade, click the down arrow next to your user name and then click **Create > Notebook**.

    ![](./Media/Lab3-Image05.jpg)

3.	On the **Create Notebook** pop-up window type “NYCTaxiData” in the Name field.
4.	Ensure you have the **Language** field set to **Python** and the **Cluster** field is set to **MDWDatabricksCluster**.
5.	Click **Create**.

    ![](./Media/Lab3-Image06.png)

6.	On the **Cmd 1** cell, click the **Edit** button on the top right-hand corner of the cell and then click **Show Title**.
7.	Type “Setup connection to MDWDataLake storage account” in the cell title.

    ![](./Media/Lab3-Image07.png)
    ![](./Media/Lab3-Image08.png)

8.	On the **Cmd 1** cell, you will invoke the Spark API to establish a connection to your MDWDataLake storage account. For this you will need to retrieve the name and key of your MDWDataLake storage account from the Azure Portal. 

    ![](./Media/Lab3-Image15.png)

9.	Use the Python code below and replace *[your MDWDataLake storage account name]* with **mdwdatalake*suffix*** and to replace *[your MDWDataLake storage account key]* with the storage account key.

```python
spark.conf.set(
  "fs.azure.account.key.[your MDWDataLake storage account name].blob.core.windows.net",
  "[your MDWDataLake storage account key]")

```

10.	Press **Shift + Enter** to execute and create a new notebook cell. 
Set the title of the **Cmd 2** cell to “Define NYCTaxiData schema and load data into a Data Frame”

11.	In the **Cmd 2** cell, define a new **StructType** object that will contain the definition of the data frame schema.
12.	Using the schema defined above, initialise a new data frame by invoking the Spark API to read the contents of the nyctaxidata container in the MDWDataLake storage account. Use the Python code below:

```python
from pyspark.sql.types import *

nycTaxiDataSchema = StructType([
  StructField("VendorID",IntegerType(),True)
  , StructField("tpep_pickup_datetime",DateType(),True)
  , StructField("tpep_dropoff_datetime",DateType(),True)
  , StructField("passenger_count",IntegerType(),True)
  , StructField("trip_distance",DoubleType(),True)
  , StructField("RatecodeID",IntegerType(),True)
  , StructField("store_and_fwd_flag",StringType(),True)
  , StructField("PULocationID",IntegerType(),True)
  , StructField("DOLocationID",IntegerType(),True)
  , StructField("payment_type",IntegerType(),True)
  , StructField("fare_amount",DoubleType(),True)
  , StructField("extra",DoubleType(),True)
  , StructField("mta_tax",DoubleType(),True)
  , StructField("tip_amount",DoubleType(),True)
  , StructField("tolls_amount",DoubleType(),True)
  , StructField("improvement_surcharge",DoubleType(),True)
  , StructField("total_amount",DoubleType(),True)])
  
dfNYCTaxiData = spark.read.format('csv').options(header='true', schema=nycTaxiDataSchema).load('wasbs://nyctaxidata@[your MDWDataLake storage account name].blob.core.windows.net/')
```

13.	Remember to replace *[your MDWDataLake storage account name]* with **mdwdatalake*suffix***. Your **Cmd 2** cell should look like this:

    ![](./Media/Lab3-Image09.png)

14.	Hit **Shift + Enter** to execute the command and create a new cell. 
15.	Set the title of the **Cmd 3** cell to “Display Data Frame Content”.
16.	In the **Cmd 3** cell, call the display function to show the contents of the data frame dfNYCTaxiData. Use the Python code below:

```python
display(dfNYCTaxiData)
```
17.	Hit **Shift + Enter** to execute the command and create a new cell. You will see a data grid showing the top 1000 records from the dataframe:

    ![](./Media/Lab3-Image10.png)

18. Set the title of the **Cmd 4** cell to “Use DataFrame Operations to Filter Data”

19.	In the **Cmd 4** cell, call the **select()** method of the data frame object to select the columns "tpep_pickup_datetime", "passenger_count" and "total_amount". Then use the  **filter()** method to filter rows where "passenger_count > 6" and "total_amount > 50.0". Use the Python code below:

```python
display(dfNYCTaxiData.select("tpep_pickup_datetime", "passenger_count", "total_amount").filter("passenger_count > 6 and total_amount > 50.0"))
```

20.	Hit **Shift + Enter** to execute the command and create a new cell. 

    ![](./Media/Lab3-Image16.png)

21.	Set the title of the **Cmd 5** cell to “Create Temp View”
22.	In the **Cmd 5** cell, call the **createOrReplaceTempView** method of the data frame object to create a temporary view of the data in memory. Use the Python code below:

```python
dfNYCTaxiData.createOrReplaceTempView('NYCTaxiDataTable')
```
23.	Hit **Shift + Enter** to execute the command and create a new cell. 

24.	Set the title of the **Cmd 6** cell to “Use SQL to count NYC Taxi Data records”

25.	In the **Cmd 6** cell, change the default language to SQL using the %sql command. 

26.	Write a SQL query to retrieve the total number of records in the NYCTaxiDataTable view. Use the command below:

```sql
%sql
select count(*) from NYCTaxiDataTable
```

27.	Hit **Shift + Enter** to execute the command and create a new cell. You will see the total number of records in the data frame at the bottom of the cell.

    ![](./Media/Lab3-Image11.png)

28.	Set the title of the **Cmd 7** cell to “Use SQL to filter NYC Taxi Data records”

29.	In the **Cmd 7** cell, write a SQL query to filter taxi rides that happened on the Apr, 7th 2018 that had more than 5 passengers. Use the command below:

```sql
%sql

select cast(tpep_pickup_datetime as date) as pickup_date
  , tpep_dropoff_datetime
  , passenger_count
  , total_amount
from NYCTaxiDataTable
where cast(tpep_pickup_datetime as date) = '2018-04-07'
  and passenger_count > 5
```

30.	Hit **Shift + Enter** to execute the command and create a new cell. You will see a grid showing the filtered result set.

    ![](./Media/Lab3-Image12.png)

31.	Set the title of the **Cmd 8** cell to “Use SQL to aggregate NYC Taxi Data records and visualize data”

32.	In the **Cmd 8** cell, write a SQL query to aggregate records and return total number of rides by payment type. Use the command below:

```sql
%sql

select case payment_type
            when 1 then 'Credit card'
            when 2 then 'Cash'
            when 3 then 'No charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided trip'
        end as PaymentType
  , count(*) as TotalRideCount
from NYCTaxiDataTable
group by payment_type
order by TotalRideCount desc

```

33.	Hit **Shift + Enter** to execute the command and create a new cell. Results will be displayed in a grid in the cell.

34.	Click the **Bar chart** button to see results as a bar chart.

    ![](./Media/Lab3-Image13.png)
    ![](./Media/Lab3-Image14.png)

35. Set the title of the **Cmd 9** cell to “Load Taxi Location Data from Azure SQL Data Warehouse”.

36. Using Python, open a JDBC connection to your Azure SQL Data Warehouse and load Taxi location lookup data from the Staging.NYCTaxiLocationLookup table into a new data frame called dfLocationLookup. 

    **IMPORTANT**: Don't forget to replace the 'mdwsqlvirtualserver-suffix' with your specific Azure SQL Data Warehouse server name.

    In the same cell, create a temporary view called "NYCTaxiLocation" and display the contents of the data frame. Use the Python code below:

```python
jdbcUrl = "jdbc:sqlserver://mdwsqlvirtualserver-suffix.database.windows.net:1433;database=MDWASQLDW"
connectionProperties = {
  "user" : "mdwadmin",
  "password" : "P@ssw0rd123!",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

pushdown_query = '(select * from Staging.NYCTaxiLocationLookup) as t'
dfLookupLocation = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)

dfLookupLocation.createOrReplaceTempView('NYCTaxiLocation')

display(dfLookupLocation) 

```

37. Hit **Shift + Enter** to execute the command and create a new cell. Results will be displayed in a grid in the cell.

    ![](./Media/Lab3-Image17.png)

38. Set the title of the **Cmd 10** cell to “Combine Data Lake and Data Warehouse data frames using SQL”.

39. In the **Cmd 10** cell, write a SQL query to join the two dataframes using their view names. Write a SELECT statement to return the "Borough" column from NYCTaxiLocation view and the columns "tpep_pickup_datetime", "passenger_count" and "total_amount" from the NYCTaxiDataTable view. Use a WHERE clause to filter taxi rides that happened on the Apr, 7th of 2018 with passenger_count > 5 and total_amount > 50.0. Use the SQL command below:

```sql
%sql

select 
    pu.Borough
  , cast(tpep_pickup_datetime as date) as pickup_date
  , passenger_count
  , total_amount
from NYCTaxiDataTable as rides
  join NYCTaxiLocation as pu
    on rides.PULocationID = pu.LocationID
where cast(tpep_pickup_datetime as date) = '2018-04-07'
  and passenger_count > 5
  and total_amount > 50.0
```

40. Hit **Ctrl + Enter** to execute the command. Results will be displayed in a grid in the cell.

    ![](./Media/Lab3-Image18.png)