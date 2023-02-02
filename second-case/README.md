# Second Case

In the second case we have the following situation:

* There are 3 spreadsheets with data from 2017 to 2019 and it's necessary that:
  * Store these files in a DB
  * Use Python and SQL
  * Use a tool to do the ETL/DAG
  * Versioning

Note: The use of GCP is desired

* So the challenge is to:
  1. Import these 3 files to a DB
  2. Do the data modeling for 4 new tables, to implement:
     - Table1 - consolidated sales by year and month
     - Table2 - consolidated sales by brand and line
     - Table3 - consolidated sales by brand, year and month
     - Table4 - consolidated sales by line, year and month
  3. Create common access account to Twitter
  4. Create developer access account in Twitter
  5. Create an app and generate the token and token secret
  6. Create a process of data capture through Twitter with following parameters:
     - Keywords = "Boticário" and highest sales line name in 12/2019 from Table2
     - Recover 50 tweets most recent
     - Just in Portuguese
  7. Save the user names ant tweets text in another DB table
