# Skatt Synthetic Data to Parquet
This is a utility for generating a parquet files with test data, based off of a given skatt avro schema.

# HOWTO:

## Prerequisites:
* An avro schema from which test data should be generated, e.g `skatt-v0.68.avsc`

## Run the main method in `no.ssb.transform.SkattTransformXmlToParquet`:
```
no.ssb.transform.SkattTransformXmlToParquet <avro-schema> <batch-size> <num-of-batches> <row-group-size> <output-folder> <start-batch> delete-existing-output=<true|false>
```
e.g:
```
no.ssb.transform.SkattTransformXmlToParquet skatt-v0.59.avsc 5000 1 512 files 0 delete-existing-output=true
```

The above example will produce output like the following:
```
Deleting previous files
No previous data in output/files
Output:       files
rowGroupSize: 512 Mb
numBatches:   1
batchSize:    5000
startBatch:    0
startId:    0
Took 35.94 sec for 5000 items (0/1)
Processed file 1 of 1
Took 0.01 hours to parse and create parquet files 5000 items
```

## Upload the generated `.parquet` file to storage bucket:
**Note:** Requires write access to bucket
```bash
$ gsutil cp output/files/skatt-v0.68-5000-gen-v-001.parquet gs://ssb-data-staging/data/generated
```

## Make the test data available via a zeppelin notebook loading the data 

Per the time of writing, use this zeppelin instance (subject to change):
`https://gnxgrwusjvartbliis3ji2k3ia-dot-europe-north1.dataproc.googleusercontent.com/zeppelin/` (LDS-b)

### Paragraph 1
```
%pyspark
skatt_raw = spark.read.load("gs://ssb-data-staging/data/generated/skatt-v0.59-5000-gen-v-001.parquet")
skatt_raw.printSchema()
```

### Paragraph 2
```
%pyspark
z.show(skatt_raw)
```
