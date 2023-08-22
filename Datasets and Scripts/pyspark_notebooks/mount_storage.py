{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "e709d255-9dba-466f-a3a5-34b78d6a1cf5",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .ansiout {\n",
       "    display: block;\n",
       "    unicode-bidi: embed;\n",
       "    white-space: pre-wrap;\n",
       "    word-wrap: break-word;\n",
       "    word-break: break-all;\n",
       "    font-family: \"Source Code Pro\", \"Menlo\", monospace;;\n",
       "    font-size: 13px;\n",
       "    color: #555;\n",
       "    margin-left: 4px;\n",
       "    line-height: 19px;\n",
       "  }\n",
       "</style>\n",
       "<div class=\"ansiout\"></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "arguments": {},
       "data": "<div class=\"ansiout\"></div>",
       "datasetInfos": [
        {
         "name": "df_raw_population",
         "schema": {
          "fields": [
           {
            "metadata": {},
            "name": "indic_de,geo\\time",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "2008 ",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "2009 ",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "2010 ",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "2011 ",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "2012 ",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "2013 ",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "2014 ",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "2015 ",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "2016 ",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "2017 ",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "2018 ",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "2019 ",
            "nullable": true,
            "type": "string"
           }
          ],
          "type": "struct"
         },
         "tableIdentifier": null,
         "typeStr": "pyspark.sql.dataframe.DataFrame"
        }
       ],
       "metadata": {},
       "removedWidgets": [],
       "type": "html"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "## Mount the following data lake storage gen2 containers\n",
    "\n",
    "\n",
    "storage_account_name = \"covidreportingddl001\"\n",
    "storage_account_key  = \"e8MHkGTgSYrKkRb2TTEVO75rEmDBD0xNplVVmOyw9C4FWwdSgXq+uMIycuhFnD7c/Bi4DGS00ELo+AStU/qYOg==\"\n",
    "\n",
    "\n",
    "spark.conf.set(\n",
    "    f\"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net\",\n",
    "    f\"{storage_account_key}\")\n",
    "\n",
    "\n",
    "container_0 = \"raw\" # make sure to use the right container\n",
    "df_raw_population = spark.read.csv(f\"abfss://{ container_0}@{storage_account_name}.dfs.core.windows.net/population\", sep=r'\\t', header=True)\n",
    "\n",
    "\n",
    "container_1 = \"lookup\" # make sure to use the right container\n",
    "df_dim_country = spark.read.csv(f\"abfss://{ container_1}@{storage_account_name}.dfs.core.windows.net/dim_country\", sep=r',', header=True)\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "bc8f80c7-de64-48ee-8600-b92b16fede05",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>indic_de,geo\\time</th><th>2008 </th><th>2009 </th><th>2010 </th><th>2011 </th><th>2012 </th><th>2013 </th><th>2014 </th><th>2015 </th><th>2016 </th><th>2017 </th><th>2018 </th><th>2019 </th></tr></thead><tbody><tr><td>PC_Y0_14,AD</td><td>14.6 </td><td>14.5 </td><td>14.5 </td><td>15.5 </td><td>15.5 </td><td>15.5 </td><td>: </td><td>: </td><td>: </td><td>: </td><td>: </td><td>13.9 </td></tr><tr><td>PC_Y0_14,AL</td><td>24.1 </td><td>23.3 </td><td>22.5 </td><td>21.6 </td><td>20.7 </td><td>20.1 </td><td>19.6 </td><td>19.0 </td><td>18.5 </td><td>18.2 </td><td>17.7 </td><td>17.2 </td></tr><tr><td>PC_Y0_14,AM</td><td>19.0 </td><td>18.6 </td><td>18.3 </td><td>: </td><td>: </td><td>: </td><td>: </td><td>19.4 </td><td>19.6 </td><td>20.0 </td><td>20.2 </td><td>20.2 </td></tr><tr><td>PC_Y0_14,AT</td><td>15.4 </td><td>15.1 </td><td>14.9 </td><td>14.7 </td><td>14.6 </td><td>14.4 </td><td>14.3 </td><td>14.3 </td><td>14.3 </td><td>14.4 </td><td>14.4 </td><td>14.4 </td></tr><tr><td>PC_Y0_14,AZ</td><td>23.2 </td><td>22.6 </td><td>22.6 </td><td>22.3 </td><td>22.2 </td><td>22.3 </td><td>22.4 </td><td>22.4 </td><td>22.5 </td><td>22.6 </td><td>22.6 </td><td>22.4 </td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "PC_Y0_14,AD",
         "14.6 ",
         "14.5 ",
         "14.5 ",
         "15.5 ",
         "15.5 ",
         "15.5 ",
         ": ",
         ": ",
         ": ",
         ": ",
         ": ",
         "13.9 "
        ],
        [
         "PC_Y0_14,AL",
         "24.1 ",
         "23.3 ",
         "22.5 ",
         "21.6 ",
         "20.7 ",
         "20.1 ",
         "19.6 ",
         "19.0 ",
         "18.5 ",
         "18.2 ",
         "17.7 ",
         "17.2 "
        ],
        [
         "PC_Y0_14,AM",
         "19.0 ",
         "18.6 ",
         "18.3 ",
         ": ",
         ": ",
         ": ",
         ": ",
         "19.4 ",
         "19.6 ",
         "20.0 ",
         "20.2 ",
         "20.2 "
        ],
        [
         "PC_Y0_14,AT",
         "15.4 ",
         "15.1 ",
         "14.9 ",
         "14.7 ",
         "14.6 ",
         "14.4 ",
         "14.3 ",
         "14.3 ",
         "14.3 ",
         "14.4 ",
         "14.4 ",
         "14.4 "
        ],
        [
         "PC_Y0_14,AZ",
         "23.2 ",
         "22.6 ",
         "22.6 ",
         "22.3 ",
         "22.2 ",
         "22.3 ",
         "22.4 ",
         "22.4 ",
         "22.5 ",
         "22.6 ",
         "22.6 ",
         "22.4 "
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "indic_de,geo\\time",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "2008 ",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "2009 ",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "2010 ",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "2011 ",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "2012 ",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "2013 ",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "2014 ",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "2015 ",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "2016 ",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "2017 ",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "2018 ",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "2019 ",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "display(df_raw_population.head(5))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "de15e7c3-d9fe-4e4b-b9a1-23629f19d0fb",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .ansiout {\n",
       "    display: block;\n",
       "    unicode-bidi: embed;\n",
       "    white-space: pre-wrap;\n",
       "    word-wrap: break-word;\n",
       "    word-break: break-all;\n",
       "    font-family: \"Source Code Pro\", \"Menlo\", monospace;;\n",
       "    font-size: 13px;\n",
       "    color: #555;\n",
       "    margin-left: 4px;\n",
       "    line-height: 19px;\n",
       "  }\n",
       "</style>\n",
       "<div class=\"ansiout\"></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "arguments": {},
       "data": "<div class=\"ansiout\"></div>",
       "datasetInfos": [
        {
         "name": "df_dim_country",
         "schema": {
          "fields": [
           {
            "metadata": {},
            "name": "country",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "country_code_2_digit",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "country_code_3_digit",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "continent",
            "nullable": true,
            "type": "string"
           },
           {
            "metadata": {},
            "name": "population",
            "nullable": true,
            "type": "string"
           }
          ],
          "type": "struct"
         },
         "tableIdentifier": null,
         "typeStr": "pyspark.sql.dataframe.DataFrame"
        }
       ],
       "metadata": {},
       "removedWidgets": [],
       "type": "html"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "container_1 = \"lookup\" # make sure to use the right container\n",
    "df_dim_country = spark.read.csv(f\"abfss://{ container_1}@{storage_account_name}.dfs.core.windows.net/dim_country\", sep=r',', header=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "d700d948-49d3-4abe-b0c0-8539acafe3ad",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>country</th><th>country_code_2_digit</th><th>country_code_3_digit</th><th>continent</th><th>population</th></tr></thead><tbody><tr><td>Aruba</td><td>AW</td><td>ABW</td><td>America</td><td>106766</td></tr><tr><td>Afghanistan</td><td>AF</td><td>AFG</td><td>Asia</td><td>38928341</td></tr><tr><td>Angola</td><td>AO</td><td>AGO</td><td>Africa</td><td>32866268</td></tr><tr><td>Anguilla</td><td>AI</td><td>AIA</td><td>America</td><td>15002</td></tr><tr><td>Albania</td><td>AL</td><td>ALB</td><td>Europe</td><td>2862427</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Aruba",
         "AW",
         "ABW",
         "America",
         "106766"
        ],
        [
         "Afghanistan",
         "AF",
         "AFG",
         "Asia",
         "38928341"
        ],
        [
         "Angola",
         "AO",
         "AGO",
         "Africa",
         "32866268"
        ],
        [
         "Anguilla",
         "AI",
         "AIA",
         "America",
         "15002"
        ],
        [
         "Albania",
         "AL",
         "ALB",
         "Europe",
         "2862427"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "country",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "country_code_2_digit",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "country_code_3_digit",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "continent",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "population",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "display(df_dim_country.head(5))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "9d8112fe-a8bc-4139-8802-0e6d592d433a",
     "showTitle": false,
     "title": ""
    }
   },
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "dashboards": [],
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "mount_storage",
   "widgets": {}
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
