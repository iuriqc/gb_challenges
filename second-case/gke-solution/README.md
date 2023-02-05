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
     - You can use the yaml file in the folder, after create a ssh-key and add to GitHub
        ```
        helm upgrade --install airflow apache-airflow/airflow -n airflow -f values.yaml
        ```
   - Configure yaml helm to setup a local folder and requirements to be installed in to Airflow (use the same yaml)

7. Create a service account to Airflow with BigQuery Job User role (just BQ will be used here) and a JSON key and use it in Airflow Connection (just copy the content)

8. 

9. To delete the Airflow from Kubernetes, do it:
   - Uninstall 
    ```
    helm delete airflow --namespace airflow
    ```
   - Unset configs
    ```
    > kubectl config unset clusters.gke_gb-challenge_southamerica-east1_gb-challenge-cluster

    > kubectl config unset contexts.gke_gb-challenge_southamerica-east1_gb-challenge-cluster

    > kubectl config unset users.gke_gb-challenge_southamerica-east1_gb-challenge-cluster
    ``` 