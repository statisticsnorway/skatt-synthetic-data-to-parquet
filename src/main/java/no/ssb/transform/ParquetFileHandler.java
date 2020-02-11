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

    public ParquetFileHandler(String folder, int rowGroupSize) {
        System.out.println("Output: " + folder);
        System.out.println("rowGroupSize: " + rowGroupSize + " Mb");
        ParquetProvider.Configuration parquetConfiguration = new ParquetProvider.Configuration();
        parquetConfiguration.setPageSize(8 * 1024 * 1024);
        parquetConfiguration.setRowGroupSize(rowGroupSize * 1024 * 1024);

        DataClient.Configuration clientConfiguration = new DataClient.Configuration();
        clientConfiguration.setLocation(folder + "/");

        client = DataClient.builder()
                .withParquetProvider(new ParquetProvider(parquetConfiguration))
                .withBinaryBackend(new LocalBackend(ROOT_OUTPUT_FOLDER + "/"))
                .withConfiguration(clientConfiguration)
                .build();
    }

    public void createFiles(String avroSchemaFileName, int batchSize, int numBatches) {
        int totalItemCount = batchSize * numBatches;
        Schema schema = getSchema(avroSchemaFileName);
        String filePrefix = avroSchemaFileName.split("\\.avsc")[0];

        int fileNum = 0;
        long totalTime = 0;

        SkattFieldInterceptor fieldHandler = new SkattFieldInterceptor();
        GenerateSyntheticData dataElements = new GenerateSyntheticData(schema, totalItemCount, fieldHandler);
        Iterable<GenericRecord> genericRecords = records(dataElements.iterator(), dataElements.getSchemaBuddy());
        Flowable<GenericRecord> records = Flowable.fromIterable(genericRecords);
        for (int i = 0; i < numBatches; i++) {
            long startTime = System.currentTimeMillis();
            Flowable<GenericRecord> batch = records.take(batchSize);
            save(schema, batch, String.format("%s-%d-%04d.parquet", filePrefix, batchSize, fileNum));
            long time = System.currentTimeMillis() - startTime;
            System.out.printf("Took %4.2f sec for %d items (%d/%d)%n", time/1000f, batchSize, i, numBatches);
            fileNum++;
            totalTime += time;
        }

        System.out.printf("Processed file %d of %d%n", fileNum, numBatches);
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
}
