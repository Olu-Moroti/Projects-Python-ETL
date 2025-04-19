# Task README

## Summary
The code in the file **ETL_code.py** gets a series of **json** files from a GitHub repository into a local PostgreSQL database, while also generating some reports.

## Process
The links of the json files were first gotten from GitHub directly, and then the links are copied into a list in the code.

![Repository](img/Repository.jpg?raw=true "Title")

The URLs strings were then reconstructed to reflect the "Raw" format of these json files in Github, so that they can be downloaded automatically with Python's `request` module.

The files were then loaded to python with Pandas. A PostgreSQL server was created, and then the Pandas dataframe were converted to SQL and loaded into the PostgreSQL database.

The downloaded json files are then deleted from the local computer.

**Note**: You will have to edit the code to give it your custom **database name (dbname)**, **username** and **password** for your PostgreSQL **localhost** server.

The remaining parts of the code is for some SQL report, as given in the tasks.

### Task 1 Output
![Task 1 Ouput](img/task1.jpeg?raw=true "Title")

### Task 2 Output
![Task 2 Ouput](img/task2.jpeg?raw=true "Title")

### Task 3 Output
![Task 3 Ouput](img/task3.jpg?raw=true "Title")

## Libraries used

- Pandas
- Sqlalchemy
- Psycopg2
