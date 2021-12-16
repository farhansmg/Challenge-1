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
