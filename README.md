FDIC Trans
===
The project scrapes the "Benedficial Ownership" filings published by the FDIC.

Requirements
==
* sqlalchemy
* requests
* lxml
* pyodbc

The scraper is designed to integrated with SQL Server, and the connection string
is hard coded for this database, but it would be relatively trivial to reconfigure
for another database.