import requests; # type: ignore
import pandas as pd; # type: ignore
from sqlalchemy import create_engine # type: ignore
from dotenv import load_dotenv; # type: ignore
import os;
from datetime import datetime, timedelta;
from prefect import flow, task # type: ignore
from prefect.runner.storage import LocalStorage # type: ignore
from prefect.deployments.runner import RunnerDeployment # type: ignore



#Configurations area (retrieves the REST API and the postgresql password from an env file, using os.getenv(), to avoid hard coding.
# Sets up the url and directory for Prefect, an orchestration tool)
load_dotenv();
api_key = os.getenv("API_KEY")
postgresql_password = os.getenv("PASSWORD")
base_url = "https://apps.kontur.io/events/v1";
os.environ['PREFECT_API_URL'] = "http://127.0.0.1:4200/api"
os.environ["PREFECT_HOME"] = "D:/Coding/ETL project/.prefect"







#Extraction (Constructs an URL using the base url, the time-window of the past 24-hours, the REST_API_KEY and other requirements. 
#Requests the HTTP to collect the json data and Error handling )
@task (retries = 3, retry_delay_seconds = 20)
def extract ():
    try:
        if not api_key:
            raise ValueError("API_KEY not found")

        time_window = (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%dT00:00:00Z")
        url = f"{base_url}/geojson/events?access_token={api_key}&feed=kontur-public&types=&severities=&after={time_window}&limit=15&sortOrder=ASC&episodeFilterType=LATEST"
        read = requests.get(url, timeout = 60);

        read.raise_for_status()

        return read.json();

    except requests.exceptions.Timeout:
        raise Exception("Request timeout error occurred")
    except requests.exceptions.ConnectionError:
        raise Exception("Connection error occurred")
    except requests.exceptions.HTTPError as x:
        raise Exception(f"HTTP error occurred : {x}")
    except requests.exceptions.RequestException as x:
        raise Exception(f"request error occurred : {x}")
    except ValueError as x:
        raise Exception(f"Value validation error occurred: {x}")
    except Exception as x:
        raise Exception(f"Something unexpected occurred : {x}")



#Transformation (Utilizes a for-loop since the json data contains dictionaries of contents inside of a "properties" dictionary, that is inside of a "features" dictionary.
# The for-loop appends all column contents to a list and later returns it. Includes error handling)
@task 
def transform(data):

    try:
        if not isinstance(data, dict):
            raise ValueError("Dataset must be in dictionary form")
        actual_data = data.get("features", [])

        transformed_data = []
        for x in actual_data:
            properties = x.get("properties", {})
            Disaster_name = properties.get("episode_name", "N/A").replace('\u20b9', 'INR')

            transformed_data.append({
                "Episode Name" : Disaster_name,
                "Description" : properties.get("episode_description", "N/A"),
                "Type" : properties.get("episode_type", "N/A"),
                "Severity" : properties.get("episode_severity", "N/A"),
                "Start Date" : properties.get("episode_startedAt", "N/A"),
                "Location" : properties.get("episode_location", "N/A"),
                "Link" : properties.get("episode_urls", "N/A")[0],
                "Magnitude" : properties.get("episode_severityData", "N/A").get("magnitude", "N/A"),
                "Depth" : properties.get("episode_severityData", "N/A").get("depthKm", "N/A"),
            })

        return transformed_data

    except Exception as x:
        raise Exception(f"Something unexpected occurred : {x}")



#Load (Creates a dataframe using the Pandas library, drops any duplicates based on the subset and keeping the first occurences.
# Build a connection to postgresql through SQLAlchemy engine and appends the transformed data into "Live Disasters" new/existing table. Includes error handling)
@task 
def load_to_postgresql(transformed_data):
    try:
        if not isinstance(transformed_data, list):
            raise ValueError("The transformed data must be in list form")
        
        dataframe = pd.DataFrame(transformed_data);
        dataframe = dataframe.drop_duplicates(subset=["Episode Name", "Start Date", "Location", "Magnitude", "Depth"], keep="first")


        engine = create_engine(f"postgresql://postgres:{postgresql_password}@localhost:5432/Disasters");
        table_name = "Live Disasters";
        dataframe.to_sql(table_name, engine, if_exists = 'append', index = False);

        
        read_from_db2 = pd.read_sql_table (table_name, engine);

        
        return read_from_db2;

    except Exception as x:
        raise Exception(f"Something unexpected occurred : {x}")



#Initiation (Calls the extract() function then passes the result the transform() to process it. Lastly, pass the result of the transformed data to load_to_postgresql())
@flow (name = "ETL")
def ETL ():
    try:
        data = extract();
        transformed_data = transform(data);
        load = load_to_postgresql(transformed_data)

        print(load)
        return load
    
    except Exception as x:
        raise Exception(f"ETL pipeline failed : {x}")




#Automization (RunnerDeployment will construct a deployment inside of prefect to orchestrate the @flow --> @task (extract) --> @task (transform) --> @task (load) every 30 minutes interval.
#work_pool_name, storage, and entrypoint will also be needed to locate the code and execution. )
if __name__ == "__main__":
    deployment = RunnerDeployment(name = "live-disasters-pipeline",
                                  flow = ETL,
                                  flow_name = "ETL",
                                  interval = (timedelta(minutes = 30)),
                                  work_pool_name="process",
                                  storage=LocalStorage(path="D:/Coding/ETL project"),
                                  entrypoint="main.py:ETL")
                            
    ETL()
    deployment.apply()
    


