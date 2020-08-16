
## Udacity Data Engineer Nano Degree Project-3

## Data Warehouse

# 1.Overview

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we were tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

#  2.Project Steps
<ul>

<li><a href="#1"> 2.1 Create Table Schemas </a></li>

*  Design schemas for fact and dimension tables 

*  Write a SQL CREATE statement for each of these tables in `sql_queries.py` 

*  Complete the logic in `create_tables.py` to connect to the database and create these tables

*  Launch a redshift cluster and create an IAM role that has read access to S3

*  Add redshift database and IAM role info to `dwh.cfg`

<li><a href="#3"> 2.2 Build ETL Pipeline </a></li>

*  Implement the logic in `etl.py` to load data from S3 to staging tables on Redshift.

*  Test by running `etl.py` after running `create_tables.py` and running analytic queries on Redshift database to compare results with the expected results.

*  Delete redshift cluster when finished


</ul>



<a id='1'></a>
###  Design schemas for fact and dimension tables

Data model used in this project:

* Fact Table: (songplays)
  Is the central table in a star schema of a data warehouse. A fact table stores quantitative   information for analysis and is often denormalized.

* Dimension Tables:(song, user, time, artist)
  They used to describe dimensions; they contain dimension keys, values and attributes.

* Staging Tables: staging_events, staging_songs

![Database ER diagram3](https://user-images.githubusercontent.com/24846149/90339813-2abcf280-dffc-11ea-9806-0e9357bb4cbb.png)











#### Note
The `SERIAL` command in Postgres is not supported in Redshift. The equivalent in redshift is `IDENTITY(0,1)`




#### Project structure
This is the project structure, if the bullet contains /
means that the resource is a folder:

/img - Simply a folder with images that are used in this md
create_tables.py - This script will drop old tables (if exist) ad re-create new tables
etl.py - This script executes the queries that extract JSON data from the S3 bucket and ingest them to Redshift
sql_queries.py - This file contains variables with SQL statement in String formats, partitioned by CREATE, DROP, COPY and INSERT statements
dhw.cfg - Configuration file used that contains info about Redshift, IAM and