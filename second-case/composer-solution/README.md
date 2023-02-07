# Case 2 - Composer Solution

1. Install Terraform

2. Install Docker

3. Create an service account at GCP for Terraform

4. Use following commands:
    
    ```console
    terraform init
    terraform apply
    ```
5. You can usually check the current state:

    ```
    terraform show
    ```
6. These steps will create an instance of Composer, a serverless Airflow at GCP.

7. After, just upload the DAG folder (contain the DAG script and your dependencies) and run it.
   > :heavy_exclamation_mark: GCP will automatic create a bucket in GCS for Airflow and you need to put the script files there.

8. To destroy just:

    ```
    terraform destroy
    ```

> :warning: In order not to incur unnecessary costs remember to check the APIs in use in GCP and to delete the bucket created for Airflow in Cloud Storage.