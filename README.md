# Private Investment in AI
This repository contains supporting code for CSET's report [Private Investment in AI](https://cset.georgetown.edu/research/tracking-ai-investment/). The code queries business investment data from Crunchbase and Refinitiv databases to identify private AI companies and calculate the investment flows into these companies.  

The script `investment_calculation.py` runs the whole calculation from the raw data stored in the CSET's Google Cloud BigQuery tables. This code can be viewed to see the steps and methods for data analysis, but it cannot be replicated outside CSET because it relies on the proprietary data with restricted access. The script `investment_calculation_replication.py` can be used for replication using the table of masked investment deals `data/masked_inv.csv` included in this repository.


# Replication of the Report:

1.) Make a new virtualenv:

```bash
python3 -m venv venv
source venv/bin/activate
```

2.) Install required Python packages by running:

`pip install -r requirements.txt`

3.) Run the script:
`python investment_calculation_replication.py`

# Description of Steps in the CSET Internal Calculation:

4.) export GOOGLE_APPLICATION_CREDENTIALS=<path to your credentials> - a service account json. You should have at least BQ reader permissions. Make sure this file is included as an enviromental variable.
 
5.) Run main script:
`python investment_calculation.py --update_analysis_data`

The script will do the following:

a. Runs SQL queries to create tables in BigQuery <br>
b. Transfers these tables to the Google Cloud Storage<br>
c. Downloads tables to hard drive in the `Data` folder<br>
d. Cleans the data<br>
e. Makes tables for the report <br>
f. Runs additional tests, which provide information used in the report<br>
g. Uploads the resulting tables to Google Cloud Storage<br>


