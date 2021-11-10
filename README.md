# Cloud-Infra-HW4
CMU 14848 Cloud Infrastructure Homework 4

## Create Jar File
```
mvn clean install
```
This will also run the test and generate the output locally.

## Create Hadoop Cluster on GCP
![Capture](https://i.imgur.com/VonasMx.jpg)

## Upload Jar File and Input to Cloud Storage
![Capture](https://i.imgur.com/uZAsFWU.jpg)

## Copy Files to Cluster
![Capture](https://i.imgur.com/FqwlKci.jpg)

## Copy from Local to HDFS
![Capture](https://i.imgur.com/T28lDks.jpg)

## Run MapReduce
```
hadoop jar hw4.jar MaxTemperature /data /output
```
![Capture](https://i.imgur.com/9yEPn3b.jpg)
![Capture](https://i.imgur.com/wGafMfI.jpg)
![Capture](https://i.imgur.com/vY6TP8s.jpg)

Sort the result
![Capture](https://i.imgur.com/rpGAEku.jpg)