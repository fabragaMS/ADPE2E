#Lab 2: Transform Big Data using Azure Data Factory and Azure SQL Data Warehouse
In this lab you will use Azure Data Factory to download large data files to your data lake and use Azure SQL Data Warehouse to generate a summary dataset and store it. The dataset you will use contains detailed New York City Yellow Taxi rides for 2018. You will generate a daily aggregated summary of all rides and save the result in your data warehouse. You will use Power BI to visualise summarised taxi ride data.

## Lab Architecture
![Lab Architecture](./Media/Lab2-Image01.png)

Step     | Description
-------- | -----
![](./Media/Green1.png) | Build an Azure Data Factory Pipeline to copy big data files from shared Azure Storage
![](./Media/Green2.png) | Save data files to your data lake
![](./Media/Green3.png) | Use Polybase to load data into staging tables in your Azure SQL Data Warehouse. Call a Stored Procedure to perform data aggregations and save results in the final table.
![](./Media/Green4.png) | Visualize data from your Azure SQL Data Warehouse using Power BI

**IMPORTANT**: Some of the Azure services provisioned require globally unique name and a “-suffix” has been added to their names to ensure this uniqueness. Please take note of the suffix generated as you will need it for the following resources:

Name	                     |Type
-----------------------------|--------------------
mdwcosmosdb-*suffix*	     |Cosmos DB account
MDWDataFactory-*suffix*	     |Data Factory (V2)
mdwdatalake*suffix*	         |Storage Account
MDWEventHubs-*suffix*	     |Event Hubs Namespace
MDWKeyVault-*suffix*	     |Key vault
mdwsqlvirtualserver-*suffix* |SQL server
MDWStreamAnalytics-*suffix*	 |Stream Analytics job



## Create Azure SQL Data Warehouse database objects
In this section you will connect to Azure SQL Data Warehouse to create the database objects used to host and process data.

![](./Media/Lab2-Image02.jpg)

**IMPORTANT**|
-------------|
**Execute these steps inside the MDWDesktop remote desktop connection**|

**IMPORTANT**: The full solution script can be found [here](./Solution/Create Staging NYCTaxiData.sql)

1.	Open Azure Data Studio. 
2.	If you already have a connection to MDWSQLVirtualServer, then **go to step 6**.
3.	On the **Servers** panel, click **New Connection**.

![](./Media/Lab2-Image03.png)

4.	On the Connection Details panel, enter the following connection details:
    <br>- **Server**: mdwsqlvirtualserver-*suffix*.database.windows.net
    <br>- **Authentication Type**: SQL Login
    <br>- **User Name**: mdwadmin
    <br>- **Password**: P@ssw0rd123!
    <br>- **Database**: MDWASQLDW
5.	Click **Connect**.

![](./Media/Lab2-Image04.png)

6.	Right click the MDWSQLVirtualServer name and then click **New Query**.

7.	Create a new round robin distributed table named Staging.NYCTaxiData. Use the script below:

```sql
create schema [Staging]
go

create table [Staging].[NYCTaxiData]
(
    [VendorID] [int] NULL,
    [tpep_pickup_datetime] [datetime] NULL,
    [tpep_dropoff_datetime] [datetime] NULL,
    [passenger_count] [smallint] NULL,
    [trip_distance] [decimal](8, 2) NULL,
    [RatecodeID] [smallint] NULL,
    [store_and_fwd_flag] [char](1) NULL,
    [PULocationID] [int] NULL,
    [DOLocationID] [int] NULL,
    [payment_type] [smallint] NULL,
    [fare_amount] [decimal](10, 2) NULL,
    [extra] [decimal](10, 2) NULL,
    [mta_tax] [decimal](10, 2) NULL,
    [tip_amount] [decimal](10, 2) NULL,
    [tolls_amount] [decimal](10, 2) NULL,
    [improvement_surcharge] [decimal](10, 2) NULL,
    [total_amount] [decimal](10, 2) NULL
)
with
(
    distribution = round_robin,
    heap
)
go

```

8.	Create a new table called Staging.NYCTaxiLocationLookup to host NYC taxi location lookup data. Use the script below:

```sql
create table [Staging].[NYCTaxiLocationLookup]
(
    [LocationID] [int] NULL,
    [Borough] [varchar](200) NULL,
    [Zone] [varchar](200) NULL,
    [service_zone] [varchar](200) NULL
)
with
(
    distribution = round_robin,
    clustered columnstore index
)
```

9.	Create a stored procedure that will transform and aggregate data and save the result in a new table called NYC.TaxiDataSummary . Use the script below:

```sql
create procedure Staging.spNYCLoadTaxiDataSummary
as
    --Drop and re-create Staging.idx_NYCTaxiData index
    if (exists(select top 1 1 from sys.indexes where name = 'idx_NYCTaxiData' and object_id = object_id('Staging.NYCTaxiData')))
        drop index idx_NYCTaxiData on Staging.NYCTaxiData 
    
    create index idx_NYCTaxiData on Staging.NYCTaxiData(tpep_pickup_datetime, PULocationID, payment_type, passenger_count, trip_distance, tip_amount, fare_amount, total_amount)

    --Drop and re-create NYC.TaxiDataSummary table
    if (exists(select top 1 1 from sys.objects where name = 'TaxiDataSummary' and schema_id = schema_id('NYC') and type = 'U'))
        drop table NYC.TaxiDataSummary 

    create table NYC.TaxiDataSummary 
    with (
        distribution = round_robin
    )
    as
    select 
        cast(tpep_pickup_datetime as date) as PickUpDate
        , PickUp.Borough as PickUpBorough
        , PickUp.Zone as PickUpZone
        , case payment_type
            when 1 then 'Credit card'
            when 2 then 'Cash'
            when 3 then 'No charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided trip'
        end as PaymentType
        , count(*) as TotalTripCount
        , sum(passenger_count) as TotalPassengerCount
        , sum(trip_distance) as TotalDistanceTravelled
        , sum(tip_amount) as TotalTipAmount
        , sum(fare_amount) as TotalFareAmount
        , sum(total_amount) as TotalTripAmount
    from Staging.NYCTaxiData
        inner join Staging.NYCTaxiLocationLookup as PickUp
            on NYCTaxiData.PULocationID = PickUp.LocationID
    group by cast(tpep_pickup_datetime as date) 
        , PickUp.Borough
        , PickUp.Zone
        , payment_type

    --drop index idx_NYCTaxiData so it does not impact future loads
    drop index idx_NYCTaxiData on Staging.NYCTaxiData
go
```

## Create NYCTaxiData Container on Azure Blob Storage
In this section you will create a container in your MDWDataLake that will be used as a repository for the NYC Taxi Data files. You will copy 12 files from the MDWResources Storage Account into your NYCTaxiData container. These files contain data for all Yellow Taxi rides in 2018, one file for each month of the year.

![](./Media/Lab2-Image05.jpg)

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	In the Azure Portal, go to the lab resource group and locate the Azure Storage account **mdwdatalake*suffix***. 
2.	On the **Overview** panel, click **Blobs**.

![](./Media/Lab2-Image06.png)

3.	On the **mdwdalalake*suffix* – Blobs** blade, click **+ Container**.

![](./Media/Lab2-Image07.png)

4.	On the New container blade, enter the following details:
    <br>- **Name**: nyctaxidata
    <br>- **Public access level**: Private (no anonymous access)
5.	Click **OK** to create the new container.

![](./Media/Lab2-Image08.png)

## Create Linked Service connection to MDWResources
In this section you will create a linked service connection to a shared storage accounnt called MDWResources hosted in an external Azure subscription. This storage account hosts the NYC Taxi data files you will copy to your data lake. As this storage account sits in an external subscription you will connect to it using a SAS URL token.

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	Open the **Azure Data Factory** portal and click the **Author option *(pencil icon)*** on the left-hand side panel. Under **Connections** tab, click **Linked Services** and then click **+ New** to create a new linked service connection.

![](./Media/Lab2-Image09.png)

2.	On the **New Linked Service** blade, type “Azure Blob Storage” in the search box to find the **Azure Blob Storage** linked service. Click **Continue**.

![](./Media/Lab2-Image10.png)

3.	On the **New Linked Service (Azure Blob Storage)** blade, enter the following details:
    <br>- **Name**: MDWResources
    <br>- **Connect via integration runtime**: AutoResolveIntegrationRuntime
    <br>- **Authentication method**: SAS URI
    <br>- **SAS URL**: 
    ```
    https://mdwresources.blob.core.windows.net/?sv=2018-03-28&ss=b&srt=sco&sp=rwl&se=2050-12-30T17:25:52Z&st=2019-04-05T09:25:52Z&spr=https&sig=4qrD8NmhaSmRFu2gKja67ayohfIDEQH3LdVMa2Utykc%3D
    ```
4.	Click **Test connection** to make sure you entered the correct connection details and then click **Finish**.

![](./Media/Lab2-Image11.png)

## Create Source and Destination Data Sets
In this section you are going to create 5 datasets that will be used by your data pipeline:
Dataset | Description
--------|---------------
**MDWResources_NYCTaxiData**| References MDWResources shared storage account container that contains source data files.
**MDWResources_NYCTaxiLookup**| References MDWResources shared storage account that contains a .csv file with all taxi location codes and names.
**MDWASQLDW_StagingNYCTaxiData**| References the table Staging.NYCTaxiData in the Azure SQL Data Warehouse database MDWASQLDW.
**MDWASQLDW_StagingNYCLocationLookup**| References the table [Staging].[NYCTaxiLocationLookup] in the Azure SQL Data Warehouse database MDWASQLDW and acts as destination of lookup data copied from MDWResources_NYCTaxiLookup.
**MDWDataLake_NYCTaxiData**| References your MDWDataLake-*suffix* storage account. It acts as the destination for the files copied from MDWResources_NYCTaxiData. It also functions as a data source when copying data to MDWASQLDW_StagingNYCTaxiData.

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	Open the **Azure Data Factory** portal and click the **Author *(pencil icon)*** option on the left-hand side panel. Under **Factory Resources** tab, click the ellipsis **(…)** next to **Datasets** and then click **Add Dataset** to create a new dataset.

![](./Media/Lab2-Image12.png)

2.	Type “Azure Blob Storage” in the search box and select **Azure Blob Storage**. Click **Continue**.

![](./Media/Lab2-Image13.png)

3.	On the **Select Format** blade, select **Binary** and click **Continue**.

![](./Media/Lab2-Image14.png)

4.	On the New Data Set tab, enter the following details:
    <br>- **General > Name**: MDWResources_NYCTaxiData
    <br>- **Connection > Linked Service**: MDWResources
    <br>- **Connection > File Path**: nyctaxidata / *.csv
    <br>- **Connection > Binary Copy**: Checked
5.	Leave remaining fields with default values.
