# Azure Data Factory COVID-19 Reporting

### Concept of the Project 💡
- This project involves the acquisition of several Covid-19 Datasets from the ECDC website. These datasets are subsequently processed through diverse ADF components to effect transformations. These transformations are executed using ADF, HDInsight, and Databricks. The resultant data is then loaded into SQL Datawarehouse with the intention of enabling the Analytics team to draw meaningful and practical insights from these datasets. The primary objective is to comprehensively understand the influence of Covid on the entirety of the European Region throughout the course of the year 2020.

### Task 🎯
- This project's mission is to ingest data from multiple data sources, clean it up, and alter it so that it is more robust and suitable for the goal. Once the data has been cleaned, it should be imported into a central repository, such as a data warehouse or datalake, so that the analytics team can utilize Power BI and other BI tools to access it. The data warehouse will contain information on confirmed cases, regrettable death rates, hospitalization and ICU cases from our weekly data lake counts, as well as testing statistics. Additionally, we can utilize these data to construct ML Models that forecast the spread of COVID in the European region.

### Source Data: 📤
- ECDC (https://www.ecdc.europa.eu/en/covid-19)
- Population Data From Azure Blob Storage (eurostat_data)

### Destination: 📥📍
- Azure Data Lake Gen2 Storage

## Tools ⚙

### Data Integration/Ingestion

- ADF Data Flows within the Data Factory

### Transformation

- Data Flows within the Data Factory
- DataBricks

### Data Warehouse Solution

- Azure SQL Database

### Visualization

- Power BI Desktop
- Power BI Service

### All the steps performed in this project are available as images in the [Covidreporting_Azure_Screenshots](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/tree/main/Covidreporting_Azure_Screenshots) folder in this repository.

### Approach

### Environment Setup
- Azure Subscription
- Data Factory
- Azure Blob Storage Account
- Data Lake Storage Gen2
- Azure SQL Database
- Azure Databricks Cluster
- HD Insight Cluster

# Solution Architecture Overview
![Solution](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/blob/main/Covidreporting_Azure_Screenshots/3.Environment_Setup/SOLUTION_ARCH.png)

### 1. Data Extraction/Data Ingestion
Four different datasets were ingested from both the ECDC website and azure blob storage into Datalake Gen2. They are - 

- Cases and Deaths Data
- Hospital Admissions Data
- Population Data
- Test Conducted Data

We used various components of ADF Pipeline activities to ingest the data from both HTTP Data Source and Azure Storage Account to Azure DataLake. Some of those activities are;

- Validation Activity
- Get Metadata Activity
- Copy Activity

### Population Data : Load into Storage Account and move it to Destination Data Lake
Ingest "population by age" data for all EU Countries into the Data Lake to support the machine learning models with the data to predict an increase in Covid-19 mortality rate.

### Solution Flow
![SolutionFlow](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/blob/main/Covidreporting_Azure_Screenshots/4.Data%20Ingestion%20from%20Blob/Module_Solution.jpeg)

### Steps:
1. Create a Linked Service To Azure Blob Storage
2. Create a Source Data Set
3. Create a Linked Service To Azure Data Lake storage (GEN2)
4. Create a Sink Data set
5. Create a Pipeline:
- Execute Copy activity when the file becomes available
- Check metadata counts before loading the data using the IF Condition
- Finally Load Data into our destination
6. ScheduleTrigger


### Pipeline Design :
![Pipeline](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/blob/main/Covidreporting_Azure_Screenshots/4.Data%20Ingestion%20from%20Blob/13.delete_after_copy.png)

### ECDC Data from Web to Destination Data Lake

### ECDC Data Content - Four files of CSV :
1. Case & Deaths Data.csv
2. Hospital Admission Data.csv
3. testing.csv
4. country_response.csv


### Solution Flow
![SOLUTION](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/4f853edd-61b2-4479-be1f-aca81605fbcf)

Steps:
1. Create a Linked Service using an HTTP connector
2. Create a Source Data Set
3. Create a Linked Service To Azure Data Lake storage (GEN2)
4. Create a Sink Data set
5. Create a Pipeline With Parameters & Variables
6. Lookup to get all the parameters from json file, then pass it to ForEach ECDC DATA as shown below
7. Schedule Trigger

![Screenshot 2023-08-22 112844](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/1db319c4-03ad-4187-b19e-73ab9517a651)

### Pipeline Design :

![13 changes_madeto_pl](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/ed161f6b-2e54-45d0-908a-d7ffcf680b27)

# 2. DATA TRANSFORMATION




















