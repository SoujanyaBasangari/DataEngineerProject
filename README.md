#The following steps are performed for building data pipeline for TLCDataset

##Understanding DataSet
TLC has published millions of trip records from both yellow medallion taxis and green SHLs. Publicizing trip record data through an open platform permits instant access to records which previously were available only through a formal process (FOIL request.) The TLC does not collect any information regarding its passengers.
The yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.

##Data Preparation - Python scripts for downlading TLC datasets for latest three years and save in nextcloud
##Install Prefect locally
## ETL pipeline with Python(ETL_local.py)
	conda create — name prefect_env python=3.8
                        conda activate prefect_env     
	conda install -c conda-forge prefect
         
                   *   extract —Extract Datasets from cloud using pyspark and create two separate datasets for yellow taxi and green taxi datasets.Add taxitype column while extracting to dataframe
                   *   transform —Add pickup_hour and dropoff_hour columns for yellow_dataset and green_dataset. Transform datasets to column oriented and row oriented and save in the path
                   *   load — Merge both sets and load data to SQL server
                  

##Utilized Prefect to declare tasks, flows, parameters, schedules and handle failures(./01-TLCData_prefect.ipynb)
      Developed a Scheduled Data Pipeline with Prefect

##Run Prefect in Saturn Cloud(./02-TLCData_prefect-cloud.ipynb)
     Connect to Prefect Cloud and orchestrate a flow running from Saturn Cloud.

# Execution Steps:
     *  To Test ETL locally
                        run   python ETL_local.py (replace path with local path)
    *   To start, sign up for a free version of a Prefect Cloud account.
    *   Upload project to cloud or  create a project and name it TLCData.
    *   Before going over to Saturn Cloud, you’ll have to create an API key in Prefect that will connect the two. You’ll find the API Key option under settings.
    *    Create a  Saturn Cloud  free account.
    *    Open a JupyterLab instance by clicking on the button, Update APi keys in TLCData_prefect.ipynb and TLCData_prefect-cloud.ipynb
    *    Run the project and check overview in prefect

# Questions

Structure the data in that way so that the data science team is able to perform their predictions:

## The input data is spread over several files, including separate files for “Yellow” and “Green” taxis. Does it make sense to merge those input file into one?

       Yes, The input file is merged in to one dataframe with an extra column taxi_type(yellow,green). It makes easier for data analysis(with respective of both schema) and loading data into one main table(taxi_table). We can maintain one main taxi schema for both types. 

##You will notice that the input data contains “date and time” columns. Your colleagues want to evaluate data also on hour-level and day of week-level. Does that affect your output-structure in some way?

      Without changing the output structure, this kind of questions can be answered by using datetime functions on table. To make transparent to answer this question pickuphour hour and dropoffhour columns are added. Structure can be further modified by adding week column as well.Right now to answer questions with respective to week i have used datetime functions