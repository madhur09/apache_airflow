Apache airflow project
===============


Introduction
---
Airflow pipeline to fetch daily COVID-19 cases data for indian states and store them into google BigTable.


Setup Airflow
---
```
1. Install Airflow.
  `pip install apache-airflow`

2. Install google cloud dependency.
  `pip install 'apache-airflow[gcp]'`

3. Create a airflow_home folder.

4. Set airflow path by opening terminal and go to `airflow_home` folder and set airflow_path using this command:-
   `export AIRFLOW_HOME=$(pwd)/airflow_home`

5. Then write `airflow initdb`. It will create airflow database, logs and unittest file.

6. Create a `dags` folder inside `airflow_home` folder to store the dags.

7. To start the server, run the commands below:
    *   `airflow webserver`
    *   `airflow scheduler`

8. Go to `localhost:8080` in your browser you will see some example dags there.
```


How to run project on local laptop
---
```
Follow the below mentioned points so you run correct main file:
    
1. Create a json credentials for google bigQuery api and paste them into config folder.
2. Create a google cloud service connection inside your airflow.
3. Create variable in airflow using the json inside config folder.
4. Paste project dags inside your dag folder in airflow_home folder.
```
