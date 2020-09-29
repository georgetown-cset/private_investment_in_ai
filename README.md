# Private Investment in AI
This repository contains a supporting code for CSET's report Private Investmnet in AI. The code queries business investment data from Crunchbased and Refinitiv to identify private AI companies and calculate the investment flows into these companies.  

The script `investment_calculation.py`runs the whole calculation from the raw data stored in the CSET's Google Cloud BigQuery tables. This code can be viewed to see the steps and methods for data analysis, but it cannot be used for replication because it relies on the proprietary data with restricted access. The script  `investment_calculation_replication.py` replicates the data tables using the table  `\data\masked_inv.csv` of masked investment deals included in this repositary.


# To Replicate Results in the Report:

1.) Make a new virtualenv:
`
python3 -m venv venv
source venv/bin/activate
`

2.) Install required Python packages by running:

`pip install -r requirements.txt`

3.) Run the script by:
`python investment_calculation_replication.py`

# Description of Steps in the CSET Internal Calculation:

4.) export GOOGLE_APPLICATION_CREDENTIALS=<path to your credentials> - a service account json. You should have at least BQ reader permissions. Make sure this file is included as an enviromental variable.
 
5.) Run main script:
`python investment_calculation.py --update_analysis_data`

The script will do the following:

a. Runs SQL queries to create tables in BQ.
b. Transferts these tables to the GCP storage.
c. Downloads tables to hard drive in the `Data` folder.
d. Cleans the data
e. Makes tables for the report 
f. Runs additional tests, which provided information used in the report
g. Uploads the resulting tables to the GCP Storage.


