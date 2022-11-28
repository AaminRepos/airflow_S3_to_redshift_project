A music streaming company, Sparkify, has decided that it is time to introduce
 more automation and monitoring to their data warehouse ETL pipelines and
 come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in S3 and needs to be processed in Sparkify's data
 warehouse in Amazon Redshift. The source datasets consist of JSON logs that
 tell about user activity in the application and JSON metadata about the songs
 the users listen to.

-Log data: s3://udacity-dend/log_data
-Song data: s3://udacity-dend/song_data

Then we create an ELT pipeline to a relational database on a 
AWS redshift cluster using the sparkify start-up files on Amazon reshift 
and apache airflow to facilitae the staging of redshift cluster on AWS, 
copying the data from the original sparkify's AWS S3 storage buckets, load 
and transform the data to fit our fact vs dimenions star schema
database design and finally run a check on each table.

This will be a one dag multiple tasks process ELT, this repository includes 2 
other folders, "dags" Which contains the dags we create or have created and
"plugins" which contains our custome made operators in "operators", SQL
statements in "helpers" which we will call upon them all in our "dags".

Operators:

* the custome made operators are the .py files in the "operators" folder.

1)stage_redshift.py > copies the data from the from the events and songs
buckets on AWS to our redshift cluster.
-
2)load_fact.py > extract the fact, songplays tables using both the events and song 
tables on our redshift cluster
-
3)load_dimensio > extract the other 4 dimensions tables using sql statments within
our helpers sql_queries.py file
-
4)data _quality > run a data check on all fact and dimensions tables by running
a Query against on the tables in our cluster.
-

Helpers:

* the .py files in the "helpers" folder.

1) sql_queries.py > conains all the premade sql statments needed to create 
our star schema from the two main evebts and son tablesusing the custom made operators above.

dags:

* the .py files in the "dags" folder.

1)sparkify_flow_dag.py > this is our main dag code where our ELT dag is defined
and the operators are being called upon to execute the funstions and the sql insert statment from the operators and the helpers folders.
-                                                                                       
Here you can see the used fact vs dimention star schema,
with the songplaystable as the fact table and the 4 other tables 
users, songs, artists and time) as the dimension tables.

Below is the exact schema used:

-Fact Table- (songplays)

coloumns = (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

-Dimension Tables-

users - users in the app
coloumns = (user_id, first_name, last_name, gender, level)

songs - songs in music database
coloumns = (song_id, title, artist_id, year, duration)

artists - artists in music database
coloumns = (artist_id, name, location, lattitude, longitude)

time - timestamps of records in songplays broken down into specific units
coloumns = (start_time, hour, day, week, month, year, weekday)

The create_tables.sql file contains the data design SQL query to create the files we require for our data warehouse from the json files.
