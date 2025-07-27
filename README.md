# Live-Disasters-Data-Pipeline
By Minh Thang Nguyen


# Project Proposal

As of recently, many places around the world experienced life endangering disasters that happened more frequently and unexpected. Some examples of these disasters include Severe flooding and earthquakes worldwide, Wild fires in the US, Cyclones in South East Asia, and more to come in the years onwards. As a result, I will come up with a way to extract these data, store them in a database, and in the future utilze AI to analyze the data to make accurate predictions. 


# Project Overview

This ETL pipeline will first run the process of extraction by collecting data from the Kontur Events Rest API. Transforming/Cleaning the raw data into a structured format and then storing it into a postgresql database. The orchestration tool, Prefect, is used to automate the pipeline every 30 minutes to ensure a live report of disasters worldwide.

# Features

* Extraction: Fetches disaster event data from a constructed URL using the base Kontur API URL and adding credentials such as an API_KEY, time_window, etc.

* Transformation: Processes JSON raw data into a structured format to function with pandas, handling missing values and cleaning special characters, .
  
* Loading: Stores transformed data in a PostgreSQL database, removing duplicates to maintain data cleanliness.
  
* Automation: Uses Prefect to schedule and orchestrate the pipeline every 30 minutes.
  
* Error Handling: Includes error handling for API requests, data validation, and database operations.

# Technologies 

* Python: Core programming language for the ETL pipeline.
  
* Prefect: Workflow orchestration for scheduling and managing the pipeline.
  
* Pandas: Data manipulation and transformation.
  
* SQLAlchemy: Database connection and interaction with PostgreSQL.
  
* Requests: HTTP requests to fetch data from the Kontur API.
  
* PostgreSQL: Relational database for storing disaster data.
  
* python-dotenv: Environment variable management for secure API key and database credentials.
