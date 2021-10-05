# Groceries data pipeline
As part of my fitness quest (?), Ive decided to analyze my consuming behaviour to measure the quality of the food im eating. To do so, ive created this data pipeline which extracts the tickets from my local supermarket as pdf, parse them and post them into postgres.

### Extract
This is done via the Page class using Selenium. Basically I scrap their page to download the supermarket tickets as .pdf.

### Transform
The first transformation I do is to convert the .pdf to .txt using the pdftotext program via bash script. Then i use the TicketsParser class to read the .txt to memory and create the SQL statements.

### Load
With the same TicketsParser class i load the data into postgres.

### Orchestation
I use Airflow to execute the different scripts everyday as set in the DAG.