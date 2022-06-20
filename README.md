# KAP---Konto-Analyse
Automatic login and download of account statements and storage in the database

1. login to the bank's website with Selenium. 
2. navigation to overview of account transactions and download of account statements (last week) in .CSV format
3. loading the account statements (.CSV) as a dataframe
4. saving of the account statements in database (MySQL) with previous check of the last contained entry of the database with entries of the current account statement 
