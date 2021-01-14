# Lab 2: Transform Big Data using Azure Data Factory Mapping Data Flows
In this lab the dataset you will use contains detailed New York City Yellow Taxi rides for the first half of 2019. You will use Azure Data Factory to download large data files to your data lake. You will generate a daily aggregated summary of all rides from data lake using Mapping Data Flows and save the resulting dataset in your Azure Synapse Analytics. You will use Power BI to visualise summarised taxi ride data.

The estimated time to complete this lab is: **60 minutes**.

## Microsoft Learn & Technical Documentation

The following Azure services will be used in this lab. If you need further training resources or access to technical documentation please find in the table below links to Microsoft Learn and to each service's Technical Documentation.

Azure Service | Microsoft Learn | Technical Documentation|
--------------|-----------------|------------------------|
Azure Data Lake Gen2 | [Large Scale Data Processing with Azure Data Lake Storage Gen2](https://docs.microsoft.com/en-us/learn/paths/data-processing-with-azure-adls/) | [Azure Data Lake Gen2 Technical Documentation](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
Azure Data Factory | [Data ingestion with Azure Data Factory](https://docs.microsoft.com/en-us/learn/modules/data-ingestion-with-azure-data-factory/)| [Azure Data Factory Technical Documentation](https://docs.microsoft.com/en-us/azure/data-factory/)
Azure Synapse Analytics | [Implement a Data Warehouse with Azure Synapse Analytics](https://docs.microsoft.com/en-us/learn/paths/implement-sql-data-warehouse/) | [Azure Synapse Analytics Technical Documentation](https://docs.microsoft.com/en-us/azure/sql-data-warehouse/)

## Lab Architecture
![Lab Architecture](./Media/Lab2-Image01.png)

Step     | Description
-------- | -----
![](./Media/Green1.png) | Build an Azure Data Factory Pipeline to copy big data files (NYC Taxi rides) from shared Azure Storage
![](./Media/Green2.png) | Ingest data files into your data lake
![](./Media/Green3.png) | Use Mapping Data Flows to generate a aggregated daily summary and save the resulting dataset into your Azure Synapse Analytics data warehouse.
![](./Media/Green4.png) | Visualize data from your Azure Synapse Analytics using Power BI

**IMPORTANT**: Some of the Azure services provisioned require globally unique name and a “-suffix” has been added to their names to ensure this uniqueness. Please take note of the suffix generated as you will need it for the following resources in this lab:

Name	                     |Type
-----------------------------|--------------------
SynapseDataFactory-*suffix*	     |Data Factory (V2)
synapsedatalake*suffix*	         |Storage Account
synapsesql-*suffix* |SQL server

## Create Azure Synapse Analytics database objects
In this section you will connect to Azure Synapse Analytics to create the data warehouse objects used to host and process data.

![](./Media/Lab2-Image02.jpg)

**IMPORTANT**|
-------------|
**Execute these steps inside the ADPDesktop remote desktop connection**|

1.	Open Azure Data Studio. 
2.	If you already have a connection to SynapseSQL endpoint, then **go to step 6**.
3.	On the **Servers** panel, click **New Connection**.

    ![](./Media/Lab2-Image03.png)

4.	On the Connection Details panel, enter the following connection details:
    <br>- **Server**: synapsesql-*suffix*.database.windows.net
    <br>- **Authentication Type**: SQL Login
    <br>- **User Name**: adpadmin
    <br>- **Password**: P@ssw0rd123!
    <br>- **Database**: SynapseDW
5.	Click **Connect**.

    ![](./Media/Lab2-Image04.png)

6.	Right click the SynapseSQL endpoint name and then click **New Query**.

    ![](./Media/Lab2-Image75.png)

7.	Create two new round robin distributed tables named [NYC].[TaxiDataSummary] and [NYC].[TaxiLocationLookup]. Use the script below:

    ```sql
    create table [NYC].[TaxiDataSummary]
    (
        [PickUpDate] [date] NULL,
        [PickUpBorough] [varchar](200) NULL,
        [PickUpZone] [varchar](200) NULL,
        [PaymentType] [varchar](11) NULL,
        [TotalTripCount] [int] NULL,
        [TotalPassengerCount] [int] NULL,
        [TotalDistanceTravelled] [decimal](38, 2) NULL,
        [TotalTipAmount] [decimal](38, 2) NULL,
        [TotalFareAmount] [decimal](38, 2) NULL,
        [TotalTripAmount] [decimal](38, 2) NULL
    )
    with
    (
        distribution = round_robin,
        clustered columnstore index
    )

    go

    create table [NYC].[TaxiLocationLookup]
    (
        [LocationID] [int] NULL,
        [Borough] [varchar](200) NULL,
        [Zone] [varchar](200) NULL,
        [ServiceZone] [varchar](200) NULL
    )
    with
    (
        distribution = round_robin,
        clustered columnstore index
    )
    go
    ```

## Create Linked Service connection to MDWResources
In this section you will create a linked service connection to a shared storage accounnt called MDWResources hosted in an external Azure subscription. This storage account hosts the NYC Taxi data files you will copy to your data lake. As this storage account sits in an external subscription you will connect to it using a SAS URL token.

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	Open the **Azure Data Factory** portal and click the **Manage option *(toolcase icon)*** on the left-hand side panel. Under **Linked services** menu item, click **+ New** to create a new linked service connection.

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

Dataset |Role           |Linked Service| Description|
--------|---------------|----------------|---------------
**MDWResources_NYCTaxiData_Binary**| Source |MDWResources|References MDWResources shared storage account container that contains source NYC Taxi data files.
**SynapseDataLake_NYCTaxiData_Binary**| Destination |synapsedatalake|References your synapsedatalake-*suffix* storage account. It acts as the destination for the NYC Taxi data files copied from MDWResources_NYCTaxiData_Binary.
**NYCDataSets_NYCTaxiLocationLookup**| Source |OperationalSQL_NYCDataSets|References [NYC].[TaxiLocationLookup] table on the NYCDataSets database. This table contains records with all taxi location codes and names.
**SynapseDW_NYCTaxiLocationLookup**| Destination|SynapseSQL_SynapseDW|References the destination table [NYC].[TaxiLocationLookup] in the Azure Synapse Analytics data warehouse SynapseDW and acts as destination of lookup data copied from NYCDataSets_NYCTaxiLookup.
**SynapseDataLake_NYCTaxiData_CSV**| Source |synapsedatalake| References the NYCTaxiData-Raw container in your SynapseDataLake-*suffix* storage account. It functions as a data source for the Mapping Data Flow.
**SynapseDW_NYCTaxiDataSummary**|SynapseSQL_SynapseDW| Destination | References the table [NYC].[TaxiDataSummary] in the Azure Synapse Analytics and acts as the destination for the summary data generated by your Mapping Data Flow.

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	Open the **Azure Data Factory** portal and click the **Author *(pencil icon)*** option on the left-hand side panel. Under **Factory Resources** tab, click the ellipsis **(…)** next to **Datasets** and then click **Add Dataset** to create a new dataset.

    ![](./Media/Lab2-Image12.png)

2.	Type “Azure Blob Storage” in the search box and select **Azure Blob Storage**. Click **Continue**.

    ![](./Media/Lab2-Image13.png)

3.	On the **Select Format** blade, select **Binary** and click **Continue**.

    ![](./Media/Lab2-Image14.png)

4.	On the **Set Properties** blade, enter the following details:
    <br>- **Name**: MDWResources_NYCTaxiData_Binary
    <br>- **Linked service**: MDWResources
    <br>- **File Path**: **Container**: nyctaxidata, **Directory**: [blank], **File**: [blank]
    
    ![](./Media/Lab2-Image41.png)

    Alternatively you can copy and paste the Dataset JSON definition below:

    ```json
    {
        "name": "MDWResources_NYCTaxiData_Binary",
        "properties": {
            "linkedServiceName": {
                "referenceName": "MDWResources",
                "type": "LinkedServiceReference"
            },
            "folder": {
                "name": "Lab2"
            },
            "annotations": [],
            "type": "Binary",
            "typeProperties": {
                "location": {
                    "type": "AzureBlobStorageLocation",
                    "container": "nyctaxidata"
                }
            }
        }
    }
    ```
5.	Leave remaining fields with default values.

    ![](./Media/Lab2-Image15.png)

6.	Repeat the process to create another Azure Storage Binary dataset, this time referencing the NYCTaxiData container in your SynapseDataLake storage account. This dataset acts as the destination for the NYC taxi data files you will copy from the previous dataset.

7.	Type “Azure Blob Storage” in the search box and select **Azure Blob Storage**. Click **Continue**.

    ![](./Media/Lab2-Image13.png)

8.	On the **Select Format** blade, select **Binary** and click **Continue**.

    ![](./Media/Lab2-Image14.png)

9.	On the **Set Properties** blade, enter the following details:
    <br>- **Name**: SynapseDataLake_NYCTaxiData_Binary
    <br>- **Linked Service**: SynapseDataLake
    <br>- **File Path**: **Container**: nyctaxidata-raw, **Directory**: [blank], **File**: [blank]

    ![](./Media/Lab2-Image42.png)

    Click **Continue**.

    Alternatively you can copy and paste the Dataset JSON definition below:

    ```json
    {
        "name": "SynapseDataLake_NYCTaxiData_Binary",
        "properties": {
            "linkedServiceName": {
                "referenceName": "SynapseDataLake",
                "type": "LinkedServiceReference"
            },
            "folder": {
                "name": "Lab2"
            },
            "annotations": [],
            "type": "Binary",
            "typeProperties": {
                "location": {
                    "type": "AzureBlobStorageLocation",
                    "container": "nyctaxidata-raw"
                }
            }
        }
    }
    ```
10.	Leave remaining fields with default values.

    ![](./Media/Lab2-Image16.png)

11.	Repeat the process to create a new Azure Storage CSV dataset referencing the NYCTaxiData container in your SynapseDataLake storage account. This dataset acts as the data source of NYC taxi records (CSV) you will copy to your Azure Synapse Analytics.

    **IMPORTANT**: You will need to download the sample file from https://aka.ms/TaxiDataSampleFile to your Desktop. This file will be used to derive the schema for the dataset. 
    
    The reason you need this step is because you will need to work with column names in the mapping data flow, but at design time you don't have the data files in your data lake yet.

12.	Type “Azure Blob Storage” in the search box and select **Azure Blob Storage**. Click **Continue**.

    ![](./Media/Lab2-Image13.png)

13.	On the **Select Format** blade, select **DelimitedText** and click **Continue**.

    ![](./Media/Lab2-Image43.png)

14.	On the **Set Properties** blade, enter the following details:
    <br>- **Name**: SynapseDataLake_NYCTaxiData_CSV
    <br>- **Linked Service**: SynapseDataLake
    <br>- **File Path**: **Container**: nyctaxidata-raw, **Directory**: [blank], **File Path**: [blank]
    <br>- **First row as header**: Checked
    <br>- **Import schema**: From sample file > [select the sample file you downloaded in step 11]

    ![](./Media/Lab2-Image44.png)

    Click **Continue**.

    Alternatively you can copy and paste the Dataset JSON definition below:

    ```json
    {
        "name": "SynapseDataLake_NYCTaxiData_CSV",
        "properties": {
            "linkedServiceName": {
                "referenceName": "SynapseDataLake",
                "type": "LinkedServiceReference"
            },
            "annotations": [],
            "type": "DelimitedText",
            "typeProperties": {
                "location": {
                    "type": "AzureBlobStorageLocation",
                    "container": "nyctaxidata-raw"
                },
                "columnDelimiter": ",",
                "escapeChar": "\\",
                "firstRowAsHeader": true,
                "quoteChar": "\""
            },
            "schema": [
                {
                    "name": "VendorID",
                    "type": "String"
                },
                {
                    "name": "tpep_pickup_datetime",
                    "type": "String"
                },
                {
                    "name": "tpep_dropoff_datetime",
                    "type": "String"
                },
                {
                    "name": "passenger_count",
                    "type": "String"
                },
                {
                    "name": "trip_distance",
                    "type": "String"
                },
                {
                    "name": "RatecodeID",
                    "type": "String"
                },
                {
                    "name": "store_and_fwd_flag",
                    "type": "String"
                },
                {
                    "name": "PULocationID",
                    "type": "String"
                },
                {
                    "name": "DOLocationID",
                    "type": "String"
                },
                {
                    "name": "payment_type",
                    "type": "String"
                },
                {
                    "name": "fare_amount",
                    "type": "String"
                },
                {
                    "name": "extra",
                    "type": "String"
                },
                {
                    "name": "mta_tax",
                    "type": "String"
                },
                {
                    "name": "tip_amount",
                    "type": "String"
                },
                {
                    "name": "tolls_amount",
                    "type": "String"
                },
                {
                    "name": "improvement_surcharge",
                    "type": "String"
                },
                {
                    "name": "total_amount",
                    "type": "String"
                },
                {
                    "name": "congestion_surcharge",
                    "type": "String"
                }
            ]
        }
    }
    ```
15.	Repeat the process to create an Azure SQL Database dataset. It references the NYC.TaxiLocationLookup table in the NYCDataSets database.

16.	Type “Azure SQL Database" in the search box and select **Azure SQL Database**. Click **Continue**.

    ![](./Media/Lab2-Image76.png)

17.	On the **Set Properties** blade, enter the following details:
    <br>- **Name**: NYCDataSets_NYCTaxiLocationLookup
    <br>- **Linked Service**: OperationalSQL_NYCDataSets
    <br>- **Table name**: NYC.TaxiLocationLookup
    <br>- **Import schema**: None.

    ![](./Media/Lab2-Image47.png)

18.	Leave remaining fields with default values.  

    Alternatively you can copy and paste the Dataset JSON definition below:

    ```json
    {
        "name": "NYCDataSets_NYCTaxiLocationLookup",
        "properties": {
            "linkedServiceName": {
                "referenceName": "OperationalSQL_NYCDataSets",
                "type": "LinkedServiceReference"
            },
            "folder": {
                "name": "Lab2"
            },
            "annotations": [],
            "type": "AzureSqlTable",
            "schema": [],
            "typeProperties": {
                "schema": "NYC",
                "table": "TaxiLocationLookup"
            }
        }
    }
    ```

19.	Repeat the process to create another dataset, this time referencing the NYC.TaxiDataSummary in your Azure Synapse Analytics database. 

20.	Type “Azure Synapse Analytics” in the search box and select **Azure Synapse Analytics**. Click **Continue**.

    ![](./Media/Lab2-Image17.png)

21.	On the Set Properties blade, enter the following details:
    <br>- **Name**: SynapseDW_NYCTaxiDataSummary
    <br>- **Linked Service**: SynapseSQL_SynapseDW
    <br>- **Table**: [NYC].[TaxiDataSummary]
    <br>- **Import schema**: From connection/store

    Alternatively you can copy and paste the Dataset JSON definition below:

    ```json
    {
        "name": "SynapseDW_NYCTaxiDataSummary",
        "properties": {
            "linkedServiceName": {
                "referenceName": "SynapseSQL_SynapseDW",
                "type": "LinkedServiceReference"
            },
            "folder": {
                "name": "Lab2"
            },
            "annotations": [],
            "type": "AzureSqlDWTable",
            "schema": [
                {
                    "name": "PickUpDate",
                    "type": "date"
                },
                {
                    "name": "PickUpBorough",
                    "type": "varchar"
                },
                {
                    "name": "PickUpZone",
                    "type": "varchar"
                },
                {
                    "name": "PaymentType",
                    "type": "varchar"
                },
                {
                    "name": "TotalTripCount",
                    "type": "int",
                    "precision": 10
                },
                {
                    "name": "TotalPassengerCount",
                    "type": "int",
                    "precision": 10
                },
                {
                    "name": "TotalDistanceTravelled",
                    "type": "decimal",
                    "precision": 38,
                    "scale": 2
                },
                {
                    "name": "TotalTipAmount",
                    "type": "decimal",
                    "precision": 38,
                    "scale": 2
                },
                {
                    "name": "TotalFareAmount",
                    "type": "decimal",
                    "precision": 38,
                    "scale": 2
                },
                {
                    "name": "TotalTripAmount",
                    "type": "decimal",
                    "precision": 38,
                    "scale": 2
                }
            ],
            "typeProperties": {
                "schema": "NYC",
                "table": "TaxiDataSummary"
            }
        }
    }
    ```

22.	Leave remaining fields with default values.

    ![](./Media/Lab2-Image18.png)

23.	Repeat the process to create another dataset, this time referencing the [NYC].[TaxiLocationLookup] in your Azure Synapse Analytics database. 

24.	Type “Azure Synapse Analytics” in the search box and select **Azure Synapse Analytics**. Click **Finish**.

    ![](./Media/Lab2-Image17.png)

25.	On the Set Properties blade, enter the following details:
    <br>-**Name**: SynapseDW_NYCTaxiLocationLookup
    <br>-**Linked Service**: SynapseSQL_SynapseDW
    <br>-**Table**: [NYC].[TaxiLocationLookup]
    <br>-**Import schema**: From connection/store

    Alternatively you can copy and paste the Dataset JSON definition below:

    ```json
    {
        "name": "SynapseDW_NYCTaxiLocationLookup",
        "properties": {
            "linkedServiceName": {
                "referenceName": "SynapseSQL_SynapseDW",
                "type": "LinkedServiceReference"
            },
            "folder": {
                "name": "Lab2"
            },
            "annotations": [],
            "type": "AzureSqlDWTable",
            "schema": [
                {
                    "name": "LocationID",
                    "type": "int",
                    "precision": 10
                },
                {
                    "name": "Borough",
                    "type": "varchar"
                },
                {
                    "name": "Zone",
                    "type": "varchar"
                },
                {
                    "name": "service_zone",
                    "type": "varchar"
                }
            ],
            "typeProperties": {
                "schema": "NYC",
                "table": "TaxiLocationLookup"
            }
        }
    }
    ```

26.	Leave remaining fields with default values.

    ![](./Media/Lab2-Image19.png)

27. Under **Factory Resources** tab, click the ellipsis **(…)** next to **Datasets** and then click **New folder** to create a new Folder. Name it **Lab2**.

28. Drag the previously created datasets into the **Lab2** folder you just created.

    ![](./Media/Lab2-Image69.png)

29.	Publish your dataset changes by clicking the **Publish all** button.

    ![](./Media/Lab2-Image20.png)


## Create a Mapping Data Flow Integration Runtime
In this section you are going to create an integration runtime for Mapping Data Flow executions. Mapping Data Flows are executed as Spark jobs and by default a new Spark cluster will be provisioned for every execution. By creating a custom integration runtime you have the option to set the compute configuration for your Spark cluster. You can also specify a TTL (time-to-live) setting that will keep the cluster active for a period of time for faster subsequent executions.

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1. On the Azure Data Factory portal and click the **Manage *(toolcase icon)*** on the left-hand side panel. Under **Integration runtimes** tab, click **+ New** to create a new integration runtime.

    ![](./Media/Lab2-Image70.png)

2. On the **Integration runtime setup** blade, select **Azure, Self-Hosted** and click **Continue**.

    ![](./Media/Lab2-Image71.png)

3. On the next page of the **Integration runtime setup** blade, select **Azure** as the network environment and click **Continue**.

    ![](./Media/Lab2-Image72.png)

4. On the next page **Integration runtime setup** blade enter the following details:
    <br>- **Name**: MappingDataFlowsIR
    <br>- **Region**: Auto Resolve
    <br>- **Data Flow runtime > Compute type**: General Purpose
    <br>- **Data Flow runtime > Core count**: 8 (+ 8 Driver cores)
    <br>- **Data Flow runtime > Time to live**: 10 minutes

    ![](./Media/Lab2-Image73.png)

5. Click **Create** to create the integration runtime.

## Create a Mapping Data Flow
In this section you are going to create a Mapping Data Flow that will transform the Taxi detailed records into an aggreated daily summary. The Mapping Data Flow will read all records from the files stored in your SynapseDataLake account and apply a sequence of transformations before the aggregated summary can be saved into the NYC.TaxiDataSummary table in your Azure Synapse Analytics.

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1. Under **Factory Resources** tab, click the ellipsis **(…)** next to **Data Flows** and then click **New data flow** to create a new Data Flow.

    ![](./Media/Lab2-Image50.png)

2. In the **New Data Flow** blade, select **Mapping Data Flow** and click **OK**.

    Note that a new tab will be open with the design surface for the Mapping Data Flow.

    ![](./Media/Lab2-Image48.png)

3.	On the Data Flow properties, enter the following details:
    <br>- **General > Name**: TransformNYCTaxiData
    
4. On the design surface click **Add Source**. On the source properties enter the following details:
    <br>- **Source Settings > Output stream name**: TaxiDataFiles
    <br>- **Source Settings > Source dataset**: SynapseDataLake_NYCTaxiData_CSV

    ![](./Media/Lab2-Image49.png)

5. Repeat the process above and add another data source. On the source properties enter the following details:
    <br>- **Source Settings > Output stream name**: TaxiLocationLookup
    <br>- **Source Settings > Source dataset**: SynapseDW_NYCTaxiLocationLookup

    ![](./Media/Lab2-Image55.png)

6. Click on the **+** sign next to the **TaxiDataFiles** source and type "Derived Column" in the search box. Click the **Derived Column** schema modifier.

    ![](./Media/Lab2-Image51.png)

7. On the Derived Column's properties, enter the following details:
    <br>- **Derived column's settings > Output stream name**: TransformColumns

8. Still on the Derived column's settings, under the **Columns** option add the following column name and expression:
    
    * Name: **PaymentType**
    
    Click the **Enter expression...** text box and enter the following expression in the **Expression for field "PaymentType"**:

    ```
    case (payment_type == '1', 'Credit card'
        , payment_type == '2', 'Cash'
        , payment_type == '3', 'No charge'
        , payment_type == '4', 'Dispute'
        , payment_type == '5', 'Unknown'
        , payment_type == '6', 'Voided trip')
    ```

    ![](./Media/Lab2-Image54.png)

    Click the **Save and Finish** button to return to the column list.

    Click the "+" sign next to the expression for PaymentType to add a new derived column. Click **Add column** from the menu.

    ![](./Media/Lab2-Image53.png)

    Repeat the process to create the following derived columns using the names and expressions below:

    |Column Name |Expression|
    | -----| ----- |
    |PickUpDate | ```toDate(tpep_pickup_datetime,'yyyy-MM-dd')``` |
    |PickUpLocationID|```toInteger(PULocationID)```|
    |PassengerCount|```toInteger(passenger_count)```|
    |DistanceTravelled|```toDecimal(trip_distance)```|
    |TipAmount|```toDecimal(tip_amount)```|
    |FareAmount|```toDecimal(fare_amount)```|
    |TotalAmount|```toDecimal(total_amount)```|

    Alternatively you can copy the code below and paste it into the DerivedColumn script window:

    ![](./Media/Lab2-Image77.png)

    ```
    source(output(
        VendorID as string,
        tpep_pickup_datetime as string,
        tpep_dropoff_datetime as string,
        passenger_count as string,
        trip_distance as string,
        RatecodeID as string,
        store_and_fwd_flag as string,
        PULocationID as string,
        DOLocationID as string,
        payment_type as string,
        fare_amount as string,
        extra as string,
        mta_tax as string,
        tip_amount as string,
        tolls_amount as string,
        improvement_surcharge as string,
        total_amount as string,
        congestion_surcharge as string
    ),
    allowSchemaDrift: true,
    validateSchema: false) ~> TaxiDataFiles
    TaxiDataFiles derive(PaymentType = case (payment_type == '1', 'Credit card'
        , payment_type == '2', 'Cash'
        , payment_type == '3', 'No charge'
        , payment_type == '4', 'Dispute'
        , payment_type == '5', 'Unknown'
        , payment_type == '6', 'Voided trip'),
    PickUpDate = toDate(tpep_pickup_datetime,'yyyy-MM-dd'),
    PickUpLocationID = toInteger(PULocationID),
    PassengerCount = toInteger(passenger_count),
    DistanceTravelled = toDecimal(trip_distance),
    TipAmount = toDecimal(tip_amount),
    FareAmount = toDecimal(fare_amount),
    TotalAmount = toDecimal(total_amount)) ~> TransformColumns
    ```

    Your full list of derived columns should look like this:

    ![](./Media/Lab2-Image52.png)

9. Click on the **+** sign next to the **TransformColumn** transformation and type "Join" in the search box. Click the **Join** transformation.

    ![](./Media/Lab2-Image56.png)

10. On the Join properties, enter the following details:
    <br>- **Join Settings > Output stream name**: JoinPickUpLocation
    <br>- **Join Settings > Left stream**: TransformColumns
    <br>- **Join Settings > Right stream**: TaxiLocationLookup
    <br>- **Join Settings > Join type**: Inner
    <br>- **Join Settings > Join conditions > Left**: PickUpLocationID
    <br>- **Join Settings > Join conditions > Right**: LocationID

    ![](./Media/Lab2-Image57.png)

11. Click on the **+** sign next to the **JoinPickUpLocation**  transformation and type "Aggregate" in the search box. Click the **Aggregate** schema modifier.

    ![](./Media/Lab2-Image58.png)

12. On the Aggregate properties, enter the following details:
    <br>- **Aggregate Settings > Output stream name**: AggregateDailySummary
    <br>- **Aggregate Settings > Group by**: Select the following columns:
    * **PickUpDate**
    * **PaymentType**
    * **Borough**
    * **Zone**

    <br>- **Aggregate Settings > Aggregates**: Add the following columns and expressions:

    |Column Name |Expression|
    | -----| ----- |
    |TotalTripCount | ```count()``` |
    |TotalPassengerCount |```sum(PassengerCount)```|
    |TotalDistanceTravelled|```sum(DistanceTravelled)```|
    |TotalTipAmount|```sum(TipAmount)```|
    |TotalFareAmount|```sum(FareAmount)```|
    |TotalTripAmount|```sum(TotalAmount)```|
    
    Your full list of aggregates should look like this:

    ![](./Media/Lab2-Image60.png)

13. Click on the **+** sign next to the **AggregateDailySummary** transformation and type "Select" in the search box. Click the **Select** transformation.

    ![](./Media/Lab2-Image61.png)

14. On the Select properties, enter the following details:
    <br>- **Select Settings > Output stream name**: RenameColumns
    <br>- **Select Settings > Input columns**: Rename the following columns:
    * **Borough** to **PickUpBorough**
    * **Zone** to **PickUpZone**

    Leave all other columns with their default values.

    ![](./Media/Lab2-Image62.png)

15. Click on the **+** sign next to the **RenameColumns** transformation and type "Sink" in the search box. Click the **Sink** destination.

    ![](./Media/Lab2-Image63.png)

16. On the Sink properties, enter the following details:
    <br>- **Sink > Output stream name**: TaxiDataSummary
    <br>- **Sink > Sink dataset**: SynapseDW_NYCTaxiDataSummary
    <br>- **Settings > Table action**: Truncate table
    <br>- **Settings > Enable staging**: Checked

    ![](./Media/Lab2-Image64.png)

    ![](./Media/Lab2-Image66.png)

17. Save and Publish your Data Flow. Your full data flow should look like this:

    ![](./Media/Lab2-Image65.png)

## Create and Execute Pipeline
In this section you create a data factory pipeline to copy and transform data in the following sequence:

* Copy NYC Taxi CSV Data files from shared storage account **MDWResources** to your the **nyctaxidata-raw** container in your **SynapseDataLake-*suffix*** storage account;

* Copy NYC taxi location data to from the MDWResources shared account directly into the NYC.TaxiLocationLookup table in your Azure Synapse Analytics.

* Use a Mapping Dataflow to transform the source data and generate a daily summary of taxi rides. The resulting dataset will be saved in the NYC.TaxiDataSummary table in your Azure Synapse Analytics. This table is then used as a source for the Power BI report.

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	Open the **Azure Data Factory** portal and click the **Author *(pencil icon)*** option on the left-hand side panel. Under the **Factory Resources** tab, click the ellipsis **(…)** next to Pipelines and then click **Add Pipeline** to create a new dataset.

2.	On the New Pipeline tab, enter the following details:
    <br>- **General > Name**: Lab 2 - Transform NYC Taxi Data

3.	Leave remaining fields with default values.

    ![](./Media/Lab2-Image21.png)

4.	From the Activities panel, type “Copy Data” in the search box. Drag the Copy Data activity on to the design surface. This copy activity will copy data files from MDWResources to SynapseDataLake.

5.	Select the Copy Data activity and enter the following details:
    <br>- **General > Name**: Copy Taxi Data Files
    <br>- **Source > Source dataset**: MDWResources_NYCTaxiData_Binary
    <br>- **Sink > Sink dataset**: SynapseDataLake_NYCTaxiData_Binary
    <br>- **Sink > Copy Behavior**: Preserve Hierarchy

6.	Leave remaining fields with default values.

    ![](./Media/Lab2-Image22.png)
    ![](./Media/Lab2-Image23.png)

12.	Repeat the process to create another Copy Data Activity, this time to copy taxi location lookup data from MDWResources to your SQL Data Warehouse.

13.	From the Activities panel, type “Copy Data” in the search box. Drag the Copy Data activity on to the design surface.

14.	Select the Copy Data activity and enter the following details:
    <br>- **General > Name**: Copy Taxi Location Lookup
    <br>- **Source > Source dataset**: NYCDataSets_NYCTaxiLocationLookup
    <br>- **Sink > Sink dataset**: SynapseDW_NYCTaxiLocationLookup
    <br>- **Sink > Pre Copy Script**: 
    ```sql
    truncate table NYC.TaxiLocationLookup
    ```
    <br>- **Settings > Enable staging**: Checked
    <br>- **Settings > Staging account linked service**: SynapseDataLake
    <br>- **Settings > Storage Path**: polybase

15.	Leave remaining fields with default values.

    ![](./Media/Lab2-Image27.png)
    ![](./Media/Lab2-Image28.png)
    ![](./Media/Lab2-Image29.png)

16.	From the Activities panel, type “Data Flow” in the search box. Drag the Data Flow activity onto the design surface. 

17. On the **Adding Data Flow** blade, select **Use existing Data Flow**. In the **Existing Data Flow** drown-down list, select **TransformNYCTaxiData**. Click **OK**.

    ![](./Media/Lab2-Image67.png)

18.	On the Data Flow activity propertie enter the following details:
    <br>- **General > Name**: Transform NYC Taxi Data
    <br>- **Settings > Run on (Azure IR)**: MappingDataFlowsIR
    <br>- **Settings > Polybase > Staging linked service**: SynapseDataLake
    <br>- **Settings > Polybase > Staging storage folder**: polybase / [blank]

    ![](./Media/Lab2-Image74.png)

19. Create two **Success *(green)*** precendence constraints between **Copy Taxi Data Files** and **Transform NYC Taxi Data** and between **Copy Taxi Location Lookup** and **Transform NYC Taxi Data**. You can do it by draggind the green square from one activity into the next one.

    ![](./Media/Lab2-Image68.png)

20.	Publish your pipeline changes by clicking the **Publish all** button.

    ![](./Media/Lab2-Image33.png)

21.	To execute the pipeline, click on **Add trigger** menu and then **Trigger Now**.

    ![](./Media/Lab2-Image34.png)

22.	On the **Pipeline Run** blade, click **Finish**.

23.	To monitor the execution of your pipeline, click on the **Monitor** menu on the left-hand side panel.

24.	You should be able to see the Status of your pipeline execution on the right-hand side panel.

    ![](./Media/Lab2-Image35.png)

25.	Click the **View Activity Runs** button for detailed information about each activity execution in the pipeline. The first execution should last between 9-12 minutes because of the Spark cluster start up time. Subsequent executions should be faster, provided they run within the TTL configured.

    ![](./Media/Lab2-Image36.png)
    ![](./Media/Lab2-Image37.png)

## Visualize Data with Power BI
In this section you are going to use Power BI to visualize data from Azure Synapse Analytics. The Power BI report will use an Import connection to query Azure Synapse Analytics and visualise Motor Vehicle Collision data from the table you loaded in the previous exercise.

**IMPORTANT**|
-------------|
**Execute these steps inside the ADPDesktop remote desktop connection**|

1.	On ADPDesktop, download the Power BI report from the link https://aka.ms/ADPLab2 and save it in the Desktop.
2.	Open the file ADPLab2.pbit with Power BI Desktop.
3.	When prompted to enter the value of the **SynapseSQLEndpoint** parameter, type the full server name: **synapsesql-*suffix*.database.windows.net**
4.	Click **Load**.

    ![](./Media/Lab2-Image38.png)

5.	When prompted to enter credentials, select **Database** from the left-hand side panel and enter the following details:
    <br>- **User name**: adpadmin
    <br>- **Password**: P@ssw0rd123!
6.	Leave remaining fields with their default values.
7.	Click **Connect**.

    ![](./Media/Lab2-Image39.png)

8.	Once data finish loading interact with the report by changing the PickUpDate slicer and by clicking on the other visualisations.
9.	Save your work and close Power BI Desktop.

    ![](./Media/Lab2-Image40.png)
