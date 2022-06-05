# Bloom Filter implementation in Hadoop

1. Build the package with maven

```bash
mvn clean package
```

2. Set the set the desired false positive rate (p) and the job stage parameters in the configuration file `config.properties`

3. Upload the [IMDb film dataset with ratings here](dataset/film-ratings.txt) to the Hadoop Distributed File System (HDFS) in the directory specified in the config.properties file

4. Run the jar file **with dependencies** by:

```bash
hadoop jar hadoop-1.0-SNAPSHOT.jar it.unipi.dii.hadoop.Driver
```
