# First Case

In the first case we have the definition of an architecture for a complex environment in a retail company.

Key points about the current environment:

* SAP Hana is the main DW repository
* There are 50 transactional data sources ==> >90% from different DBMS and on-premises
* Beyond SAP Hana, there are hosted applications in public clouds like AWS and Azure
* Inside the company there are silos, using different tools to process, analyse and show data like Jupyter Notebook and Qlik
* Data Governance improvements to be applied to sensitive data, data cataloging and permissioning

And...

Key points about the desired environment:

* Defined architecture in a public cloud preferably AWS or GCP with:
    - Pass through layers of ingestion, processing, storage, consumption, analysis, security and governance;
    - Gradual replacement of on-premises;
    - Include technologies to enable real-time data analysis;
    - The architecture must have components that allow the company to organize and supply data to Analytics, Data Science,
    API's and services for integration with applications. It's emphasized that necessarily the company needs to keep the 
    communication between on-premises and cloud to various purposes.
* Detail the motivation for choosing the components, with their advantages, disadvantages and risks
