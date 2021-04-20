# Synthetic Data to Parquet
This is a utility for generating a parquet files with test data, based off of a given avro schema.

# HOWTO:
## build
* mvn clean install
* If libs are missing, this can be build locally by building them in [avro-buddy-project](https://github.com/statisticsnorway/avro-buddy-project)

## Prerequisites:
* An avro schema from which test data should be generated, e.g `sample.avsc`
* A FieldInterceptor, se `SamleFieldInterceptor` as example

If libs are missing, this can be build locally by building them in [avro-buddy-project](https://github.com/statisticsnorway/avro-buddy-project)   

## Run the main method in `no.ssb.transform.TransformXmlToParquet`:
```
no.ssb.transform.TransformXmlToParquet.<field-interceptor> <avro-schema> <batch-size> <num-of-batches> <row-group-size> <output-folder> <start-batch> delete-existing-output=<true|false>
```
e.g:
```
java -jar target/synthetic-data-to-parquet-1.0-SNAPSHOT.jar no.ssb.transform.SampleFieldInterceptor sample.avsc 10 1 256 sample-files 0 delete-existing-output=true
```

The above example will produce output like the following:
```
Deleting output/sample-files
interceptor:  SampleFieldInterceptor
Output:       sample-files
rowGroupSize: 256 Mb
numBatches:   1
batchSize:    10
startBatch:    0
startId:    0
Took 0.54 sec for 10 items (0/1)
Processed file 1 of 1
Took 0.00 hours to parse and create parquet files 10 items
```

## Field Interception
A good tool for finding and adding field interception is to run the the test `GenerateSyntheticDataTest.printSchemaWithValues`
It will print schema with values for 1 line of the test data for the provided schema
e.g:
```
spark_schema value:null
 |-- fnr value:1000000001
 |-- count value:10
```