# Bloom Filter implementation in Spark

1. Build the venv, it is required that all the nodes in the cluster have the same Python interpreter installed
```bash
python -m venv pyspark_venv
```

2. Install the packages in the venv
```bash
pip3 install pyspark mmh3 bitarray
```

3. Export the venv
```bash
venv-pack -o pyspark_venv.tar.gz
```

4. Edit $SPARK_HOME/conf/spark-env.sh to include:
```bash
export PYSPARK_PYTHON=./pyspark_venv/bin/python
```

5. Zip the BloomFilter module
```bash
zip -r bloomfilter.zip bloomfilter
```

6. To launch the Spark application use:
```bash
spark-submit --archives pyspark_venv.tar.gz#pyspark_venv main.py
```
