##  ETL Pipelene
by Natness
![](C:\Users\Admin\Desktop\Bootcamp\W19\screenshot.png)



### Introduction

This repository guides users through each step of building an ETL pipeline.

ETL is a process that extracts, transforms, and loads data from multiple sources into a data warehouse or other unified data repository.

This project has 3 main steps:

1) Extracting data from Amazon Redshift (includes connection codes to Redshift) extract.py

2) Transformation of the data with identifying and removing duplicates transform.py

3) loading transformed data to a s3 bucket load.py

### Requirements
The minimum requirements:

Python 3+


### Instructions on how to execute the code

How to run?
To run the code main.py successfully follow the steps below: 1) Be sure to have Python3+ 2) Clone the repository, and go to the week19 folder

git clone https://github.com/aysuary/etl_pipeline.git
3) Install the necessary libraries which are listed in requirements.txt with using codes:

pip3 install -r requirements.txt
4) Copy the variable from the .env.copy file to .env and fill out the environment variables.

5) Run the main.py script

Mac users:

python3 main.py
Windows users:

python main.py