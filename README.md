# public loan data
## Introduction to Data
As part of a larger effort to increase transparency, Fannie Mae and Freddie Mac are making available loan-level credit performance data on a portion of fully amortizing fixed-rate mortgages that the company purchased or guaranteed since 2000. These data are very valuable to mortgage data analysis, data mining and machine learning. 
* FNMA Loan Data can be accessed from https://www.fanniemae.com/portal/funding-the-market/data/loan-performance-data.html
* FRED Loan Data can be accessed from http://www.freddiemac.com/research/datasets/sf_loanlevel_dataset.page

## ETL for FNMA and FRED public Loan data.
FNMA provides sample SAS and R scripts and FRED provides sample SAS script for processing downloaded datasets.  

Nowadays, Python programming get very popular. For my own research purpose, I provide 3 implementations ( **1. pandas**; **2.Dask**; **3. pyspark**) which can be used to handle both FNMA and FRED data.  
* FNMA SAS: https://loanperformancedata.fanniemae.com/lppub-docs/FNMA_SF_Loan_Performance_sas_Primary.zip
* FNMA R: https://loanperformancedata.fanniemae.com/lppub-docs/FNMA_SF_Loan_Performance_r_Primary.zip
* FRED SAS: http://www.freddiemac.com/fmac-resources/research/pdf/sas_scripts.zip

## Implemtation Platform.
For the convenience, the implementation is done by using Google Collaboratory Notebooks, so that we can easily try GPU and SPARK option.
If you are interested, you may clone  the notebook **PubLoanPops.ipynb** to your google colab to run (of cause, you are expected to download data from FNMA/FRED and upload them to your Google driver). 

