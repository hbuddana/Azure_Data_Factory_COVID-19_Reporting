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

<img src="https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/blob/main/Covidreporting_Azure_Screenshots/3.Environment_Setup/SOLUTION_ARCH.png" alt="Solution" style="border: 2px solid black;">


### DATA EXTRACTION/ INGESTION
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

The Cases and Deaths data together with the Hospital admissions data was transformed using ADF Data flows. The Data Flows transformation used on both dataset include

- Select transformation
- Lookup transformation
- Filter transformation
- Join transformation
- Sort transformation
- Conditional split transformation
- Derived columns transformation
- Sink transformation

# Data Flows (1) Cases & Deaths Data:

### Solution Flow
![Screenshot 2023-08-22 115115](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/9685b3ab-7db3-4657-b799-2ace7f38a8bc)

### Steps:
1. Cases And Deaths Source (Azure Data Lake Storage Gen2 )
2. Filter Europe-Only Data
3. Select only the required columns
4. PivotCounts using indicator Columns(confirmed cases, deaths) and get the sum of daily cases count
5. Lookup Country to get country_code_2_digit,country_code_3_digit columns
6. Select Only the required columns for the Sink
7. Create a Sink dataset (Azure Data Lake Storage Gen2)
8. Used Schedule Trigger

![6 lookup](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/b83197b6-4fff-4b59-a832-7f6c7745c361)


# Data Flows (2) Hospital Admissions Data:

### Solution Flow
![Screenshot 2023-08-22 115705](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/6936a920-d985-424c-892f-a285f8a16b11)

### Steps:
1. Hospital Admissions Source (Azure Data Lake Storage Gen2 )
2. Select only the required columns
3. Lookup Country to get country_code_2_digit,country_code_3_digit columns
4. Select only the required columns
5. Condition Split Weekly, Daily Split condition
  - indicator=='Weekly new hospital admissions per 100k' || indicator=='Weekly new ICU admissions per 100k'
  - indicator== "Daily hospital occupancy" || indicator=="Daily ICU occupancy"
6. For Weekly Path
- Join with Date to get ecdc_Year_week, week_start_date, week_End_date
- Piovt Counts using indicator Columns(confirmed cases, deaths) and get the sum of daily cases count
- Sort data using reported_year_week ASC and Country DESC
- Select only required columns for sink
- Create a sink dataset (Azure Data Lake Storage Gen2)
- Schedule Trigger
7. For Daily Path
- Pivot Counts using indicator Columns(confirmed cases, deaths) and get the sum of daily cases count
- Sort Data using reported_year_week ASC and Country DESC
- Select only required columns for sink
- Create a sink dataset (Azure Data Lake Storage Gen2)
- Used Schedule Trigger

![pllll](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/79f249e4-44a5-4480-9417-3a2417e5d44a)

# Databricks Activity (3) -- Population File:

![Screenshot 2023-08-22 120411](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/28d97faf-7242-4116-be97-002a8a41422e)

# 3. Copy Data to Azure SQL
1- Copy Cases and Deaths
2- Copy hospital admissions data
3- Copy testing data

![4 pl_succ](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/dea9e004-a556-4bc9-929a-d7239d6a9359)


![7 PL-SUCC](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/75dc0a79-fb9e-4963-97bd-4303677aa775)

# 4. Reporting via Power BI

1. Create a Connection from Azure SQL to Power Bi and load the data
2. Analyze the data to get the total confirmed cases and deaths count
3. Identify the trends in data based on reporting date
4. Publish the report to Power BI Server
5. Publish to web

# Covid-19 Trend in the EU/EEA & UK 2020 by Cases, Deaths, Hospital Occupancy, and ICU Occupancy
![Screenshot 2023-08-22 121510](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/dd18e69a-2db9-4d45-9fa9-a89c9f960af4)


# Covid-19 Cases and Death breakdown by population in the UK, France, and Germany
![Screenshot 2023-08-22 121924](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/cfea642a-8fd5-4c0f-86d6-68cf9bf687cc)



# Total Number of covid tests carried out vs Confirmed Cases
![Screenshot 2023-08-22 122141](https://github.com/hbuddana/Azure_Data_Factory_COVID-19_Reporting/assets/65592890/2b38f5f2-aef5-49a9-a828-5230708decbc)



# Power BI Dashboard
[Dashboard Link](https://app.powerbi.com/view?r=eyJrIjoiODhiN2FiZGMtMWRkMy00ZWZjLWJiNWItNjY1ZjQ5YmFjYzkwIiwidCI6IjcwZGUxOTkyLTA3YzYtNDgwZi1hMzE4LWExYWZjYmEwMzk4MyIsImMiOjN9)


# Used Technologies
- Azure DataFactory
- Azure HDinsight (Hive)
- Azure Databricks (Pyspark, SparkSql)
- Azure Storage Account
- Azure Data Lake Gen2
- Azure SQL Database
























