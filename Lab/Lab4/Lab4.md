# Lab 4: Add AI to your Big Data Pipeline with Cognitive Services
In this lab you will use Azure Data Factory to download New York City images to your data lake. Then, as part of the same pipeline, you are going to use an Azure Databricks notebook to invoke Computer Vision Cognitive Service to generate metadata documents and save them in back in your data lake. The Azure Data Factory pipeline then finishes by saving all metadata information in a Cosmos DB collection. You will use Power BI to visualise NYC images and their AI-generated metadata.

**IMPORTANT**: The typical use case for Cosmos DB is to serve as the operational database layer for data-driven applications (e.g. real-time personalisation). This lab intends to illustrates how analytics data pipelines can be used to deliver insights to intelligent apps through Cosmos DB. For the sake of keeping this lab simple we will use Power BI to query Cosmos DB data instead of an App.

The estimated time to complete this lab is: **75 minutes**.

## Microsoft Learn & Technical Documentation

The following Azure services will be used in this lab. If you need further training resources or access to technical documentation please find in the table below links to Microsoft Learn and to each service's Technical Documentation.

Azure Service | Microsoft Learn | Technical Documentation|
--------------|-----------------|------------------------|
Azure Cognitive Vision Services | [Process and classify images with the Azure Cognitive Vision Services](https://docs.microsoft.com/en-us/learn/paths/classify-images-with-vision-services/) | [Azure Computer Vision Technical Documentation](https://docs.microsoft.com/en-us/azure/cognitive-services/computer-vision/)
Azure Cosmos DB | [Work with NoSQL data in Azure Cosmos DB](https://docs.microsoft.com/en-us/learn/paths/work-with-nosql-data-in-azure-cosmos-db/) | [Azure Cosmos DB Technical Documentation](https://docs.microsoft.com/en-us/azure/cosmos-db/)
Azure Databricks | [Perform data engineering with Azure Databricks](https://docs.microsoft.com/en-us/learn/paths/data-engineering-with-databricks/) | [Azure Databricks Technical Documentation](https://docs.microsoft.com/en-us/azure/azure-databricks/)
Azure Data Lake Gen2 | [Large Scale Data Processing with Azure Data Lake Storage Gen2](https://docs.microsoft.com/en-us/learn/paths/data-processing-with-azure-adls/) | [Azure Data Lake Gen2 Technical Documentation](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)


## Lab Architecture
![Lab Architecture](./Media/Lab4-Image01.png)

Step     | Description
-------- | -----
![](./Media/Blue1.png) | Build an Azure Data Factory Pipeline to copy image files from shared Azure Storage
![](./Media/Blue2.png) | Save image files to your data lake
![](./Media/Blue3.png) | For each image in your data lake, invoke an Azure Databricks notebook that will take the image URL as parameter
![](./Media/Blue4.png) | For each image call the Azure Computer Vision Cognitive service to generate image metadata. Metadata files are saved back in your data lake
![](./Media/Blue5.png) | Copy metadata JSON documents into your Cosmos DB database
![](./Media/Blue6.png) | Visualize images and associated metadata using Power BI

**IMPORTANT**: Some of the Azure services provisioned require globally unique name and a “-suffix” has been added to their names to ensure this uniqueness. Please take note of the suffix generated as you will need it for the following resources in this lab:

Name	                     |Type
-----------------------------|--------------------
adpcosmosdb-*suffix*	     |Cosmos DB account
SynapseDataFactory-*suffix*	 |Data Factory (V2)
synapsedatalake*suffix*	     |Storage Account
ADPDatabricks-*suffix*	     |Databricks Workspace

## Create CosmosDB database and collection
In this section you will create a CosmosDB database called NYC and a collection called ImageMetadata that will host New York image metadata information. 

![](./Media/Lab4-Image07.png)

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	In the Azure Portal, go to the lab resource group and locate the CosmosDB account **ADPCosmosDB-*suffix***. 
2.	On the **Overview** panel, click **+ Add Container**.
    
    ![](./Media/Lab4-Image08.png)

3.	On the **Add Container** blade, enter the following details:
    <br>- **Database id > Create new**: NYC
    <br>- **Container id**: ImageMetadata
    <br>- **Partition key**: /requestId
    <br>- **Throughput**: 400
    <br>- **Unique keys**: /requestId
4.	Click **OK** to create the container.

    ![](./Media/Lab4-Image09.png)

## Import Databricks Notebook to Invoke Computer Vision Cognitive Services API
In this section you will import a Databricks notebook to your workspace and fill out the missing details about your Computer Vision API and your Data Lake account. This notebook will be executed from an Azure Data Factory pipeline and it will invoke the Computer Vision API to generate metadata about the images and save the result back to your data lake.

![](./Media/Lab4-Image10.png)

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	On the Azure Databricks portal, click the **Workspace** button on the left-hand side menu. 
2.	On the **Workspace** blade, click your username under the **Users** menu.
3.	On the **Users** blade, click the arrow next to your user name and then **Import**.

    ![](./Media/Lab4-Image11.png)

4.	On the **Import Notebooks** pop up window, select **Import from: URL**. Copy and paste the URL below in the box:

```url
https://github.com/fabragaMS/ADPE2E/raw/master/Lab/Lab4/NYCImageMetadata-Lab.dbc
```

5.	Click **Import**.

6.	On the NYCImageMetadata-Lab notebook, go to Cmd 2 cell **Define function to invoke Computer Vision API**. You will need to change the function code to include the Computer Vision API details.

    ![](./Media/Lab4-Image12.png)

7.	From the Azure Portal, retrieve the ADPComputerVision subscription key and base endpoint URL.

    ![](./Media/Lab4-Image13.jpg)
    ![](./Media/Lab4-Image14.png)

8.	Copy and paste the Key and Endpoint values back in the Databricks notebook.

9.	On the NYCImageMetadata-Lab notebook, go to Cmd 3 cell **Define function to mount NYC Image Metadata Container**. You will need to change the function code to include your data lake storage account details.

10.	In the **dataLakeaccountName** variable assignment replace *&lt;SynapseDataLake storage account name&gt;* with **synapsedatalake*suffix***.

    ![](./Media/Lab4-Image16.png)

11.	From the Azure Portal, retrieve the **SynapseDataLake*suffix*** access key.

    ![](./Media/Lab4-Image17.jpg)

12.	Copy and paste the Access Key Databricks notebook. Replace *&lt;SynapseDataLake storage account key&gt;* with the Access Key you got from the previous step.

13.	Attach the notebook to your previously created **ADPDatabricksCluster** cluster.

    ![](./Media/Lab4-Image19.png)

14.	Review the notebook code.

15.	If you want to test it, you can copy any publicly available image URL and paste it in the Image URL notebook parameter. You can use any of the following image URLs in the list as examples:

Test Image|Test Image URL|
----------|--------------|
![](https://petlifetoday.com/wp-content/uploads/2018/06/wireless-dog-fence.jpg)|https://petlifetoday.com/wp-content/uploads/2018/06/wireless-dog-fence.jpg|
![](https://static.pexels.com/photos/4204/nature-lawn-blur-flower.jpg)|https://static.pexels.com/photos/4204/nature-lawn-blur-flower.jpg|
![](https://image.redbull.com/rbcom/052/2017-05-22/89eef344-d24f-4520-8680-8b8f7508b264/0012/0/0/0/2428/3642/800/1/best-beginner-motocross-bikes-ktm-250-sx-f.jpg)|https://image.redbull.com/rbcom/052/2017-05-22/89eef344-d24f-4520-8680-8b8f7508b264/0012/0/0/0/2428/3642/800/1/best-beginner-motocross-bikes-ktm-250-sx-f.jpg|
![](https://www.zastavki.com/pictures/originals/2014/World___Panama_City_landscape_in_panama_079246_.jpg)|https://www.zastavki.com/pictures/originals/2014/World___Panama_City_landscape_in_panama_079246_.jpg|

16.	Click Run All to execute the notebook.

    ![](./Media/Lab4-Image20.png)

17.	After a successful execution you will notice that a new JSON file has been saved in the **NYCImageMetadata** container in your Data Lake. 

    ![](./Media/Lab4-Image21.png)

18.	Navigate to Azure Portal and check the contents of the **nycimagemetadata** container in your **SynapseDataLake*suffix*** storage account.

19.	Download the file generated to inspect its contents.

20.	**IMPORTANT**: Delete this test file before moving to next steps of this exercise.

    ![](./Media/Lab4-Image22.png)

## Create Databricks Linked Service in Azure Data Factory
In this section you will create a Databricks linked service in Azure Data Factory. Through this linked service you will be able to create a data pipelines to copy NYC images to your data lake and integrate Databricks notebooks to its execution. 

![](./Media/Lab4-Image23.png)

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	On the Azure Databricks portal, click the **User** icon on the top right-hand corner of the screen.
2.	Click on the **User Settings** menu item.

    ![](./Media/Lab4-Image24.png)

3.	On the **User Settings** blade, under the **Access Tokens** tab, click **Generate New Token**.

    ![](./Media/Lab4-Image25.png)

4.	On the **Generate New Token** pop-up window, enter “Azure Data Factory Access” in the **Comment** field. Leave **Lifetime (days)** with the default value of 90 days.
5.	Click **Generate**.

    ![](./Media/Lab4-Image26.png)

6.	**IMPORTANT**: Copy the generated access token to Notepad and save it. You won’t be able to retrieve it once you close this window.

    ![](./Media/Lab4-Image27.png)

7.	Open the **Azure Data Factory portal** and click the **Manage *(toolcase icon)*** option on the left-hand side panel. Under **Liked services** menu item, click **Linked Services** and then click **+ New** to create a new linked service connection.

    ![](./Media/Lab4-Image28.png)

8.	On the **New Linked Service** blade, click the **Compute** tab. 

9.	Type “Azure Databricks” in the search box to find the **Azure Databricks** linked service.

10.	Click **Continue**.

    ![](./Media/Lab4-Image29.png)

11.	On the **New Linked Service (Azure Databricks)** blade, enter the following details:
    <br>- **Name**: ADPDatabricks
    <br>- **Connect via integration runtime**: AutoResolveIntegrationRuntime
    <br>- **Account selection method**: From Azure subscription
    <br>- **Azure subscription**: [select your subscription]
    <br>- **Databricks workspace**: ADPDatabricks-*suffix*
    <br>- **Select cluster**: Existing interactive cluster
    <br>- **Access token**: [copy and paste previously generated access token here]
    <br>- **Choose from existing clusters**: ADPDatabricksCluster

    ![](./Media/Lab4-Image30.png)

12.	Click **Test connection** to make sure you entered the correct connection details. You should see a “Connection successful” message above the button.

13.	If the connection was successful, then click **Finish**. If you got an error message, please review the connection details above and try again.

## Create CosmosDB Linked Service in Azure Data Factory

In this section you will create a CosmosDB linked service in Azure Data Factory. CosmosDB will be used as the final repository of image metadata information generated by the Computer Vision API. Power BI will then be used to visualise the CosmosDB data.


![](./Media/Lab4-Image23.png)

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	Open the Azure Data Factory portal and click the **Manage *(toolcase icon)*** option on the left-hand side panel. Under **Linked services** menu item, click **+ New** to create a new linked service connection.

    ![](./Media/Lab4-Image28.png)

2.	On the **New Linked Service** blade, click the **Data Store** tab. 

3.	Type “Cosmos DB” in the search box to find the **Azure Cosmos DB (SQL API)** linked service.

4.	Click **Continue**.

    ![](./Media/Lab4-Image31.png)

5.	On the New Linked Service (Azure Databricks) blade, enter the following details:
    <br>- **Name**: ADPCosmosDB
    <br>- **Connect via integration runtime**: AutoResolveIntegrationRuntime
    <br>- **Account selection method**: From Azure subscription
    <br>- **Azure subscription**: [select your subscription]
    <br>- **Cosmos DB account name**: adpcosmosdb-*suffix*
    <br>- **Database name**: NYC

    ![](./Media/Lab4-Image32.png)

6.	Click **Test connection** to make sure you entered the correct connection details. You should see a “Connection successful” message above the button.

7.	If the connection was successful, then click **Finish**. If you got an error message, please review the connection details above and try again.

## Create Azure Data Factory data sets.
In this section you will create 4 Azure Data Factory data sets that will be used in the data pipeline.

Dataset | Role| Description
--------|-----|----------
**MDWResources_NYCImages_Binary**| Source | References MDWResources shared storage account container that contains source image files.
**SynapseDataLake_NYCImages_Binary**| Destination | References your synapsedataLake-*suffix* storage account and it acts as the destination for the image files copied from MDWResources_NYCImages. 
**SynapseDataLake_NYCImageMetadata_JSON**| Source | References your synapsedatalake-*suffix* storage account and it acts as the source of image metadata files (JSON) generated by Databricks and Computer Vision. 
**ADPCosmosDB_ImageMetadata**| Destination | References ADPCosmosDB-*suffix* database that will save the metadata info for all images.

![](./Media/Lab4-Image23.png)

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	Open the Azure Data Factory portal and click the **Author *(pencil icon)*** option on the left-hand side panel. Under **Factory Resources** tab, click the ellipsis **(…)** next to **Datasets** and then click **Add Dataset** to create a new dataset.

    ![](./Media/Lab4-Image33.png)

2.	Type “Azure Blob Storage” in the search box and select **Azure Blob Storage**.

    ![](./Media/Lab4-Image34.png)

3.	On the **Select Format** blade, select **Binary** and click **Continue**.

    ![](./Media/Lab4-Image35.png)

4.	On the **Set Properties** blade, enter the following details:
    <br>- **Name**: MDWResources_NYCImages_Binary
    <br>- **Linked Service**: MDWResources
    <br>- **File Path**: **Container**: nycimages, **Directory**: [blank], **File**: [blank]

    ![](./Media/Lab4-Image71.png)

    Click **Continue**.

5.	Leave remaining fields with default values.

    ![](./Media/Lab4-Image36.png)

    Alternatively you can copy and paste the dataset JSON definition below:

    ```json
    {
        "name": "MDWResources_NYCImages_Binary",
        "properties": {
            "linkedServiceName": {
                "referenceName": "MDWResources",
                "type": "LinkedServiceReference"
            },
            "folder": {
                "name": "Lab4"
            },
            "annotations": [],
            "type": "Binary",
            "typeProperties": {
                "location": {
                    "type": "AzureBlobStorageLocation",
                    "container": "nycimages"
                }
            }
        }
    }
    ```

6.	Repeat the process to create another dataset, this time referencing the **NYCImages** container in your **synapsedatalake-*suffix*** storage account. 

7.	Type “Azure Blob Storage” in the search box and click **Azure Blob Storage**.

    ![](./Media/Lab4-Image34.png)

8.	On the **Select Format** blade, select **Binary** and click **Continue**.

    ![](./Media/Lab4-Image35.png)

9.	On the **Set Properties** blade, enter the following details:
    <br>- **Name**: SynapseDataLake_NYCImages_Binary
    <br>- **Linked Service**: SynapseDataLake
    <br>- **File Path**: **Container**: nycimages, **Directory**: [blank], **File**: [blank]

    ![](./Media/Lab4-Image72.png)

    Click **Continue**.

10.	Leave remaining fields with default values.

    ![](./Media/Lab4-Image37.png)
    Alternatively you can copy and paste the dataset JSON definition below:

    ```json
    {
        "name": "SynapseDataLake_NYCImages_Binary",
        "properties": {
            "linkedServiceName": {
                "referenceName": "SynapseDataLake",
                "type": "LinkedServiceReference"
            },
            "folder": {
                "name": "Lab4"
            },
            "annotations": [],
            "type": "Binary",
            "typeProperties": {
                "location": {
                    "type": "AzureBlobStorageLocation",
                    "container": "nycimages"
                }
            }
        }
    }
    ```

11.	Repeat the process to create another dataset, this time referencing the **NYCImageMetadata** container in your **synapsedatalake-*suffix*** storage account. 

12.	Type “Azure Blob Storage” in the search box and click **Azure Blob Storage**

    ![](./Media/Lab4-Image34.png)

13.	On the **Select Format** blade, select **JSON** and click **Continue**.

    ![](./Media/Lab4-Image38.png)

14.	On the **Set properties** blade, enter the following details:
    <br>- **Name**: SynapseDataLake_NYCImageMetadata_JSON
    <br>- **Linked Service**: SynapseDataLake
    <br>- **File Path**:  **Container**: nycimagemetadata, **Directory**: [blank], **File**: [blank]
    <br>- **Import Schema**: None

    ![](./Media/Lab4-Image39.png)

    Alternatively you can copy and paste the dataset JSON definition below:

    ```json
    {
        "name": "SynapseDataLake_NYCImageMetadata_JSON",
        "properties": {
            "linkedServiceName": {
                "referenceName": "SynapseDataLake",
                "type": "LinkedServiceReference"
            },
            "folder": {
                "name": "Lab4"
            },
            "annotations": [],
            "type": "Json",
            "typeProperties": {
                "location": {
                    "type": "AzureBlobStorageLocation",
                    "container": "nycimagemetadata"
                }
            }
        }
    }
    ```
15.	Leave remaining fields with default values.

![](./Media/Lab4-Image73.png)

16.	Repeat the process to create another dataset, this time referencing the **ImageMetadata** collection in your **ADPCosmosDB** database. 

17.	Type “Cosmos DB” in the search box and select **Azure Cosmos DB (SQL API)**. Click **Continue**.

    ![](./Media/Lab4-Image40.png)

18.	On the **Set properties** blade, enter the following details:
    <br>- **Name**: ADPCosmosDB_NYCImageMetadata
    <br>- **Linked Service**: ADPCosmosDB
    <br>- **Collection**: ImageMetadata
    <br>- **Import schema**: None

    ![](./Media/Lab4-Image75.png)

    Alternatively you can copy and paste the dataset JSON definition below:

    ```json
    {
        "name": "ADPCosmosDB_NYCImageMetadata",
        "properties": {
            "linkedServiceName": {
                "referenceName": "ADPCosmosDB",
                "type": "LinkedServiceReference"
            },
            "folder": {
                "name": "Lab4"
            },
            "annotations": [],
            "type": "CosmosDbSqlApiCollection",
            "typeProperties": {
                "collectionName": "ImageMetadata"
            }
        }
    }
    ```
19.	Leave remaining fields with default values.

    ![](./Media/Lab4-Image41.png)

20. Under **Factory Resources** tab, click the ellipsis **(…)** next to **Datasets** and then click **New folder** to create a new Folder. Name it **Lab4**.

21. Drag the previously created datasets into the **Lab4** folder you just created.

    ![](./Media/Lab4-Image74.png)

22.	Publish your dataset changes by clicking the **Publish all** button.

    ![](./Media/Lab4-Image42.png)

## Create Azure Data Factory pipeline to generate and save image metadata to Cosmos DB.

In this section you will create an Azure Data Factory pipeline to copy New York images from MDWResources into your SynapseDataLakesuffix storage account. The pipeline will then execute a Databricks notebook for each image and generate a metadata file in the NYCImageMetadata container. The pipeline finishes by saving the image metadata content in a CosmosDB database.

![](./Media/Lab4-Image43.png)

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	Open the Azure Data Factory portal and click the **Author *(pencil icon)*** option on the left-hand side panel. Under **Factory Resources** tab, click the ellipsis **(…)** next to **Pipelines** and then click **Add Pipeline** to create a new pipeline.

    ![](./Media/Lab4-Image44.png)

2.	On the **New Pipeline** tab, enter the following details:
    <br>- **General > Name**: Lab4 - Generate NYC Image Metadata
    <br>- **Variables > [click + New] >**
    <br>    - **Name**: ImageMetadataContainerUrl
    <br>    - **Default Value**: https://synapsedatalake[suffix].blob.core.windows.net/nycimages/
    **IMPORTANT**: Remember to replace the suffix for your SynapseDataLake account!

3.	Leave remaining fields with default values.

    ![](./Media/Lab4-Image45.png)

4.	From the **Activities** panel, type “Copy Data” in the search box. Drag the **Copy Data** activity on to the design surface. This copy activity will copy image files from shared storage account **MDWResources** to your **SynapseDataLake** storage account.

5.	Select the **Copy Data** activity and enter the following details:
    <br>- **General > Name**: CopyImageFiles
    <br>- **Source > Source dataset**: MDWResources_NYCImages_Binary
    <br>- **Sink > Sink dataset**: SynapseDataLake_NYCImages_Binary
    <br>- **Sink > Copy Behavior**: Preserve Hierarchy

6.	Leave remaining fields with default values.

    ![](./Media/Lab4-Image46.png)
    ![](./Media/Lab4-Image47.png)

7.	From the **Activities** panel, type “Get Metadata” in the search box. Drag the **Get Metadata** activity on to the design surface. This activity will retrieve a list of image files saved in the NYCImages container by the previous CopyImageFiles activity. 

8.	Select the **Get Metadata** activity and enter the following details:
    <br>- **General > Name**: GetImageFileList
    <br>- **Dataset**: SynapseDataLake_NYCImages_Binary
    <br>- **Source > Field list**: Child Items

9.	Leave remaining fields with default values.

    ![](./Media/Lab4-Image48.png)

10.	Create a **Success *(green)*** precedence constraint between **CopyImageFiles** and **GetImageFileList** activities. You can do it by dragging the green connector from CopyImageFiles and landing the arrow onto GetImageFileList.

    ![](./Media/Lab4-Image49.png)

11.	From the **Activities** panel, type “ForEach” in the search box. Drag the **ForEach** activity on to the design surface. This ForEach activity will act as a container for other activities that will be executed in the context of each image files returned by the GetImageFileList activity. 

12.	Select the ForEach activity and enter the following details:
    <br>- **General > Name**: ForEachImage
    <br>- **Settings > Items**: 
    ```
    @activity('GetImageFileList').output.childItems
    ```

13.	Leave remaining fields with default values.

    ![](./Media/Lab4-Image50.png)

14.	Create a **Success *(green)*** precedence constraint between **GetImageFileList** and **ForEachImage** activities. You can do it by dragging the green connector from GetImageFileList and landing the arrow onto ForEachImage.

    ![](./Media/Lab4-Image51.png)

15.	Double-click the **ForEachImage** activity to edit its contents. 

    **IMPORTANT**: Note the design context is displayed on the top left-hand side of the design canvas.

    ![](./Media/Lab4-Image52.png)

16.	From the **Activities** panel, type “Notebook” in the search box. Drag the **Notebook** activity on to the design surface. This Notebook activity will pass the image URL as a parameter to the Databricks notebook we created previously.

17.	Select the **Notebook** activity and enter the following details:
    <br>- **General > Name**: GetImageMetadata
    <br>- **Azure Databricks > Databricks Linked Service**: ADPDatabricks
    <br>- **Settings > Notebook path**: [Click Browse and navigate to /Users/*your-user-name*/NYCImageMetadata-Lab]
    <br>- **Base Parameters**: [Click **+ New**]
    <br>- **Name**: **nycImageUrl**
    <br>- **Value**:
    ```
    @concat(variables('ImageMetadataContainerUrl'), item().name)
    ```
    **IMPORTANT**: Variable name is case sensitive!

18.	Leave remaining fields with default values.

    ![](./Media/Lab4-Image53.png)
    ![](./Media/Lab4-Image54.png)

19.	Navigate back to the “Copy NYC Images” pipeline canvas.

20.	From the Activities panel, type “Copy Data” in the search box. Drag the **Copy Data** activity on to the design surface. This copy activity will copy image metadata from the JSON files sitting on the NYCImageMetadata container in SynapseDataLake to the ImageMetadata collection on CosmosDB.

21.	Select the Copy Data activity and enter the following details:
    <br>- **General > Name**: ServeImageMetadata
    <br>- **Source > Source dataset**: SynapseDataLake_NYCImageMetadata_JSON
    <br>- **Source > File path type**: Wildcard file path
    <br>- **Source > Wildcard file name**: *.json
    <br>- **Sink > Sink dataset**: ADPCosmosDB_NYCImageMetadata

22.	Leave remaining fields with default values.

    ![](./Media/Lab4-Image55.png)
    ![](./Media/Lab4-Image56.png)

23.	Create a **Success *(green)*** precedence constraint between **ForEachImage** and **ServeImageMetadata** activities. You can do it by dragging the green connector from ForEachImage and landing the arrow onto ServeImageMetadata.

    ![](./Media/Lab4-Image57.png)

24.	Publish your pipeline changes by clicking the **Publish all** button.

    ![](./Media/Lab4-Image58.png)

25.	To execute the pipeline, click on **Add trigger** menu and then **Trigger Now**.

26.	On the **Pipeline Run** blade, click **Finish**.

    ![](./Media/Lab4-Image59.png)

27.	To monitor the execution of your pipeline, click on the **Monitor** menu on the left-hand side panel.

28.	You should be able to see the **Status** of your pipeline execution on the right-hand side panel.

    ![](./Media/Lab4-Image60.png)

29.	Click the **View Activity Runs** button for detailed information about each activity execution in the pipeline. The whole execution should last between 7-8 minutes.

    ![](./Media/Lab4-Image61.png)

## Explore Image Metadata Documents in CosmosDB
In this section you will explore the image metadata records generated by the Azure Data Factory pipeline in CosmosDB. You will use the Cosmos DB’s SQL API to write SQL-like queries and retrieve data based on their criteria.

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	In the Azure Portal, go to the lab resource group and locate the CosmosDB account **ADPCosmosDB-*suffix***. 

2.	On the **Data Explorer** panel, click **Open Full Screen** button on the top right-hand side of the screen.

3.	On the **Open Full Screen** pop-up window, click **Open**.

    ![](./Media/Lab4-Image62.png)

4.	On the **Azure Cosmos DB Data Explorer window**, under **NYC > ImageMetadata** click the **Items** menu item to see the full list of documents in the collection.

5.	Click any document in the list to see its contents.

    ![](./Media/Lab4-Image63.png)

6.	Click the ellipsis **(…)** next to **ImageMetadata** collection.

7.	On the pop-up menu, click **New SQL Query** to open a new query tab.

    ![](./Media/Lab4-Image64.png)

8.	On the **New Query 1** window, try the two different SQL Commands from the list. Click the **Execute Selection** button to execute your query.

```sql
SELECT m.id
    , m.imageUrl 
FROM ImageMetadata as m
```

```sql
SELECT m.id
    , m.imageUrl 
    , tags.name
FROM ImageMetadata as m
    JOIN tags IN m.tags
WHERE tags.name = 'wedding'
```

10.	Check the results in the Results panel.

    ![](./Media/Lab4-Image65.png)

## Visualize Data with Power BI
In this section you are going to use Power BI to visualize data from Cosmos DB. The Power BI report will use an Import connection to retrieve image metadata from Cosmos DB and visualise images sitting in your data lake.

**IMPORTANT**|
-------------|
**Execute these steps on your host computer**|

1.	Navigate to the Azure Portal and retrieve the **adpcosmosdb-*suffix*** access key.

2.	Save it to notepad. You will need it in the next step.

    ![](./Media/Lab4-Image66.png)

**IMPORTANT**|
-------------|
Execute these steps inside the **ADPDesktop** remote desktop connection|

1.	On ADPDesktop, download the Power BI report from the link https://aka.ms/ADPLab4 and save it in the Desktop.

2.	Open the file **ADPLab4.pbit** with Power BI Desktop.

3.	When prompted to enter the value of the **ADPCosmosDB** parameter, type the full server URI: https://adpcosmosdb-*suffix*.documents.azure.com:443/

4.	Click **Load**.

    ![](./Media/Lab4-Image68.png)

5.	When prompted for an **Account Key**, paste the ADPCosmosDB account key you retrieved in the previous exercise.

6.	Click **Connect**.

    ![](./Media/Lab4-Image69.png)

7.	Once data finish loading, interact with the report by clicking on the different images displayed and check the accuracy of their associated metadata.

8.	Save your work and close Power BI Desktop.

    ![](./Media/Lab4-Image70.png)
