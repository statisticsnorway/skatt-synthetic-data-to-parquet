# Skatt Synthetic Data to Parquet
This is a utility for generating a parquet files with test data, based off of a given skatt avro schema.

# HOWTO:

## Prerequisites:
* An avro schema from which test data should be generated, e.g `skatt-v0.68.avsc`
* A FieldInterceptor, se `SkattFieldInterceptor` as example

no.ssb.transform.FregFieldInterceptor naeringsopplysninger_v1.avsc 50 1 64 naering_v1 0 delete-existing-output=true

## Run the main method in `no.ssb.transform.SkattTransformXmlToParquet`:
```
no.ssb.transform.SkattTransformXmlToParquet.<field-interceptor> <avro-schema> <batch-size> <num-of-batches> <row-group-size> <output-folder> <start-batch> delete-existing-output=<true|false>
```
e.g:
```
java -jar target/skatt-synthetic-data-to-parquet-1.0-SNAPSHOT.jar no.ssb.transform.SkattFieldInterceptor skatt-v0.59.avsc 5000 1 512 v0.59-files 0 delete-existing-output=true
```

The above example will produce output like the following:
```
Output:       v0.59-files 
rowGroupSize: 512 Mb
numBatches:   1
batchSize:    5000
startBatch:    0
startId:    0
Took 35.94 sec for 5000 items (0/1)
Processed file 1 of 1
Took 0.01 hours to parse and create parquet files 5000 items
```

