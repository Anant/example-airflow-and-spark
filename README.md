# Airflow and Spark

Learn how to use Airflow to schedule and run Spark jobs.

We will be using [Gitpod](https://www.gitpod.io/) as our dev environment so that you can quickly learn and test without having to worry about OS inconsistencies. If you have not already opened this in gitpod, then `CTR + Click` the button below and get started! <br></br>
[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/Anant/example-airflow-and-spark) 

## 1. Set up Airflow

We will be using the quick start script that Airflow provides [here](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html).

```bash
bash setup.sh
```

## 2. Start Spark in standalone mode

### 2.1 - Start master

```bash
./spark-3.1.1-bin-hadoop2.7/sbin/start-master.sh
```

### 2.2 - Start worker

Open port 8081 in the browser, copy the master URL, and paste in the designated spot below

```bash
./spark-3.1.1-bin-hadoop2.7/sbin/start-worker.sh <master-URL>
```

## 3. Move spark_dag.py to ~/airflow/dags

### 3.1 - Create ~/airflow/dags

```bash
mkdir ~/airflow/dags
```

### 3.2 - Move spark_dag.py

```bash
mv spark_dag.py ~/airflow/dags
```

## 4, Open port 8080 to see Airflow UI and check if `example_spark_operator` exists. 
If it does not exist yet, give it a few seconds to refresh.

## 5. Update Spark Connection, unpause the `example_spark_operator`, and drill down by clicking on `example_spark_operator`.

### 5.1 - Under the `Admin` section of the menu, select `spark_default` and update the host to the Spark master URL. Save once done

### 5.2 - Select the `DAG` menu item and return to the dashboard. Unpause the `example_spark_operator`, and then click on the `example_spark_operator`link. 

## 6. Trigger the DAG from the tree view and click on the graph view afterwards

## 7. Once the jobs have run, you can click on each task in the graph view and see their logs.
In their logs, we should see value of Pi that each job calculated, and the two numbers differing between Python and Scala

## 8. Trigger DAG from command line

### 8.1 - Open a new terminal and run `airflow dags`

```bash
airflow dags trigger example_spark_operator
```

### 8.2 - If we want to trigger only one task

```bash
airflow tasks run example_spark_operator python_submit_job now
```

And that wraps up our basic walkthrough on using Airflow to schedule Spark jobs.
