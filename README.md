# Synapsis Coal Mining Data Warehouse

## Note
- high level document / brief report:
    
    https://docs.google.com/document/d/1cxIKmwQk272VGHpsj-v04sR7NjxDxVgf9T2Gi6ERt5k
- metabase dashboard:

    ![alt text](img-metabase-dashboard.png)

## Requirements
1. Docker
2. Docker compose

## How to Run
1. clone this project
2. Modify `.env` file except vars [optional]:

     ```
     *_HOSTNAME=
     API_PARAM_START_DATE=
     API_PARAM_END_DATE=
     PROJECT_DATA_DIR=
     PROJECT_QUERIES_DIR=
     QUERIES_FILENAME=
    ```
3. run `docker-compose up --build`

