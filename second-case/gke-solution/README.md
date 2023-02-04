# Case 2 - GKE Solution

1. Install Docker

2. Install Helm

3. Install gcloud cli and kubectl

4. Configure your project in gcloud and create a cluster

    ```
    > gcloud auth login

    > gcloud config set project gb-challenge

    > gcloud container clusters create gb-challenge-cluster --machine-type n1-standard-1 --num-nodes 1 --region "southamerica-east1"
    ```
5. Then to connect the cluster with local kubectl
    ```
    gcloud container clusters get-credentials gb-challenge-cluster --region southamerica-east1
    ```
6. Prepare Airflow to be installed in cluster:
   - Install Airflow with [helm](https://airflow.apache.org/docs/helm-chart/stable/index.html)
   - Access webserver:
        ```
        kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
        ```
   - Configure git-sync for your DAG folder [here](https://airflow.apache.org/docs/helm-chart/stable/manage-dags-files.html)
     - You can use the yaml file here, after create a ssh-key and add to GitHub

7. Copy files to GCS
    ```
    > gsutil -m cp -r /Users/iuriqc/Desktop/applications/gb_challenges/second-case/files/* gs://gb-challenge-bucket/files
    ```

8. Create a service account to Airflow with BigQuery Job User role (just BQ will be used here) and a JSON key and add content of the key to Secret Manager and use it in Airflow Connection (you need to grant access to service account used in Compute Engine)