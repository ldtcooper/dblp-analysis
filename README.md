# DBLP Pipeline
This is a data pipeline built as an assignment for CS 516: Database Systems at Duke University. It ingests XML data from the [DBLP Computer Science Bibliography](https://dblp.org/) and turns it into a Postgres database for analysis.

## Setup
The first step of this process is to create a conda enviornment from `a1.txt`. This can be done by running the command:
```sh
conda env create -f environment.yml
```
 After that, you can run `setup.sh` to download the appropriate data.

 Note that this project was specifically made to run correctly on WSL2.

## Running the pipeline
Assuming that you have Postgres installed and set up correctly, the first step is setting up the following tables:
- article: **pubkey** (text), journal (text), year(int)
- inproceedings: **pubkey** (text), booktitle (text), year(int)
- authorship: **pubkey** (string), **author** (string)

After that, you can run `run.sh` to convert the downloaded materials from XML.

## Analysis
Some basic SQL analysis of the database can be found in `assignment1.ipynb`.