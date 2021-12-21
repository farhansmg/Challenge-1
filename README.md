# Challenge 1
Repository of the Challenge 

question no. 4 Convert yelp dataset from json to csv using preferred code :
```
Step to run it :
* make sure code, dataset, and Dockerfile was on same directory
* docker build -t your-prefered-image-name .
* example : docker build -t convert-json .
* run it with mount your local dir so you don't have to rebuild the image
* docker run --mount type=bind,source=your-code-dir,target=/opt/application convert-json driver local:///opt/application/convert_json.py
* example : docker run --mount type=bind,source=/opt/app/,target=/opt/application convert-json driver local:///opt/application/convert_json.py
```
question no.6, if you don't have the postgre, you can running it with composer and also I've make it when you start the postgre, the DDL also be applied. Make sure docker-compose
and docker_postgres_init.sql was on the same dir.

question no.7, you have submit migrate_ods.py into spark
question no.8, same as number 7 because my staging area was an object file system so I'm not extract it from database's object but from the file it self.
