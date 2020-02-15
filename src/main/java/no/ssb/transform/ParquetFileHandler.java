package no.ssb.transform;

import io.reactivex.Flowable;
import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaAwareElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import no.ssb.avro.generate.GenerateSyntheticData;
import no.ssb.lds.data.client.DataClient;
import no.ssb.lds.data.client.LocalBackend;
import no.ssb.lds.data.client.ParquetProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class ParquetFileHandler {
    private static final String ROOT_OUTPUT_FOLDER = "output";

    private DataClient client;
    private Builder options;

    public ParquetFileHandler(Builder options) {
        this.options = options;

        System.out.println("Output: " + options.folder);
        System.out.println("rowGroupSize: " + options.rowGroupSize + " Mb");
        ParquetProvider.Configuration parquetConfiguration = new ParquetProvider.Configuration();
        parquetConfiguration.setPageSize(8 * 1024 * 1024);
        parquetConfiguration.setRowGroupSize(options.rowGroupSize * 1024 * 1024);

        DataClient.Configuration clientConfiguration = new DataClient.Configuration();
        clientConfiguration.setLocation(options.folder + "/");

        client = DataClient.builder()
                .withParquetProvider(new ParquetProvider(parquetConfiguration))
                .withBinaryBackend(new LocalBackend(ROOT_OUTPUT_FOLDER + "/"))
                .withConfiguration(clientConfiguration)
                .build();
    }

    public void createFiles() {
        int totalItemCount = options.batchSize * options.numBatches;
        Schema schema = getSchema(options.avroSchemaFileName);
        String filePrefix = options.avroSchemaFileName.split("\\.avsc")[0];

        int fileNum = 0;
        long totalTime = 0;

        SkattFieldInterceptor fieldHandler = new SkattFieldInterceptor();
        GenerateSyntheticData dataElements = new GenerateSyntheticData(schema, totalItemCount, fieldHandler, 0);
        Iterable<GenericRecord> genericRecords = records(dataElements.iterator(), dataElements.getSchemaBuddy());
        Flowable<GenericRecord> records = Flowable.fromIterable(genericRecords);
        for (int i = 0; i < options.numBatches; i++) {
            long startTime = System.currentTimeMillis();
            Flowable<GenericRecord> batch = records.take(options.batchSize);
            save(schema, batch, String.format("%s-%d-%04d.parquet", filePrefix, options.batchSize, fileNum));
            long time = System.currentTimeMillis() - startTime;
            System.out.printf("Took %4.2f sec for %d items (%d/%d)%n", time/1000f, options.batchSize, i, options.numBatches);
            fileNum++;
            totalTime += time;
        }

        System.out.printf("Processed file %d of %d%n", fileNum, options.numBatches);
        System.out.printf("Took %4.2f hours to parse and create parquet files %d items%n", totalTime/(1000f*60*60), totalItemCount);
    }

    static Schema getSchema(String avroJsonSchemaFile) {
        try {
            return new Schema.Parser().parse(new File(avroJsonSchemaFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    Iterable<GenericRecord> records(Iterator<DataElement> elementIterator, SchemaBuddy schema) {
        return () -> new Iterator<GenericRecord>() {
            @Override
            public boolean hasNext() {
                return elementIterator.hasNext();
            }

            @Override
            public GenericRecord next() {
                return SchemaAwareElement.toRecord(elementIterator.next(), schema);
            }
        };
    }

    public void save(Schema schema, Flowable<GenericRecord> records, String fileName) {
        client.writeAllData(fileName, schema, records, "").blockingAwait();
    }

    static class Builder {
        private String folder;
        private int rowGroupSize;
        private String avroSchemaFileName;
        private int batchSize;
        private int numBatches;

        public Builder addFolder(String folder) {
            this.folder = folder;
            return this;
        }

        public Builder addAvroSchemaFileName(String avroSchemaFileName) {
            this.avroSchemaFileName = avroSchemaFileName;
            return this;
        }

        public Builder addRowGroupSize(int rowGroupSize) {
            this.rowGroupSize = rowGroupSize;
            return this;
        }

        public Builder addBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }
        public Builder addNumBatches(int numBatches) {
            this.numBatches = numBatches;
            return this;
        }

        ParquetFileHandler build() {
            return new ParquetFileHandler(this);
        }
    }
}
