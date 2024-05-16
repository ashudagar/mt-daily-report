## Generate Daily Report ##

    This project is generating daily repots detailing the 
    number of lesson completed on daily basis by 
    each of its active user.
    This report is in the CSV format with the following 
    columns - Name, Number of lessons completed, Date

    System collects the data from multiple sources like
    Postgres & MySql and then using Pandas, it transforms
    the data in required format and save it in a csv file.

    Later, this csv file has been uploaded to S3 and sent 
    over to the configured recipients email too.

### How to run
This project can be run in 2 ways:
1. Directly using Python Script - manually or setting up a cron job
2. Using the Apache Airflow dag - for continuous and lager data sets

### Prerequisites
To run this project following prerequisites are required:
1. Docker Desktop
2. AWS Creds for S3 and SES
3. S3 bucket with name - mt-daily-report
4. Verified emails to send email using SES 
5. Setting up Apache Airflow - optional

### Project Setup ###
#### Main setup ####
    1. clone the repo
    2. Navigate to the `setup` directory.
    3. Copy the environment file and populate it with the required values:
        ``` bash
            cd setup
            cp .env.example .env
        ```
    4. Launch Docker Images
        Start the docker containers:
        ``` bash
            docker-compose up --build
        '''



#### Setup for running python script ####

    1. Create virtual env: python3 -m venv venv-dr
    2. Activate the virtual env: source venv-dr/bin/activate
    3. Install the requirements: pip install -r requirements.txt
    4. go to the mt-daily-report-directory: cd mt-daily-report
    5. Set values for all variables in **.env** file
    6. Run the main file: python main.py

#### Setup for running Apache Airflow Dag ####
    1. 1-5 steps are same as above
    2. Copy the dag [generate_report_dag.py](mt-daily-report%2Fgenerate_report_dag.py) & [.env](mt-daily-report%2F.env) in dags/ folder if you are running apache airflow already and it will gets executed automatically
    3. if not, then setup airflow, just run: airflow standalone
        it will start a local airflow server on 8080
        by default if will create an folder at location ~/airflow
        go to this directory: cd ~/airflow
        create a directory if not present already: mkdir dags
        go in this directory: cd dags
        copy the above 2 files at this location
        refresh your browser at http://localhost:8080
        run the daily_lesson_report_dag
Boom, Check your inbox for the daily completed lesson report for each user.
