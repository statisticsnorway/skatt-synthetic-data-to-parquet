package no.ssb.transform;

import io.reactivex.Flowable;
import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaAwareElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import no.ssb.lds.data.client.DataClient;
import no.ssb.lds.data.client.LocalBackend;
import no.ssb.lds.data.client.ParquetProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.List;

public class SkattTransformXmlToParquet {

    private static final int COUNT_TO_GENERATE = 50_000 * 100;
    private static final int BATCH_SIZE = 50_000;

    // A simple command line app to generate a parquet from random data
    public static void main(String[] args) {

        SkattSchema sourceSchema = new SkattSchema();

        SchemaBuddy schemaBuddy = SchemaBuddy.parse(sourceSchema.getRootSchema());
        createFile(sourceSchema, schemaBuddy);
    }

    private static void createFile(SkattSchema sourceSchema, SchemaBuddy schemaBuddy) {
        long startTime = System.currentTimeMillis();
        int fileNum = 0;
        List<GenericRecord> list = new ArrayList<>(COUNT_TO_GENERATE);
        long totalTime = 0;
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(sourceSchema.getRootSchema(), COUNT_TO_GENERATE);
        for (DataElement dataElement : generateSyntheticData) {
            GenericRecord record = SchemaAwareElement.toRecord(dataElement, schemaBuddy);
            list.add(record);
            if (list.size() == BATCH_SIZE) {
                System.out.printf("Took %dms to parse %d items%n", System.currentTimeMillis() - startTime, list.size());

                ParquetFileCreator parquetFileCreator = new ParquetFileCreator("data", "files");
                parquetFileCreator.save(sourceSchema.getRootSchema(), list, String.format("skatt-2-levels-v0.53-%d-%04d.parquet", BATCH_SIZE, fileNum));
                long time = System.currentTimeMillis() - startTime;
                System.out.printf("Took %dms to parse and create parquet file %d items%n", time, list.size());
                fileNum++;
                list.clear();
                totalTime += time;
                startTime = System.currentTimeMillis();

                System.out.printf("Processed %d of %d%n", fileNum, COUNT_TO_GENERATE / BATCH_SIZE);
            }
        }
        System.out.printf("Took %dms to parse and create parquet files %d items%n", totalTime, COUNT_TO_GENERATE);
    }

    public static class ParquetFileCreator {

        private DataClient client;

        public ParquetFileCreator(String prefix, String folder) {
            ParquetProvider.Configuration parquetConfiguration = new ParquetProvider.Configuration();
            parquetConfiguration.setPageSize(8 * 1024 * 1024);
            parquetConfiguration.setRowGroupSize(64 * 1024 * 1024);

            DataClient.Configuration clientConfiguration = new DataClient.Configuration();
            clientConfiguration.setLocation(folder + "/");

            client = DataClient.builder()
                    .withParquetProvider(new ParquetProvider(parquetConfiguration))
                    .withBinaryBackend(new LocalBackend(prefix + "/"))
                    .withConfiguration(clientConfiguration)
                    .build();
        }

        public void save(Schema schema, List<GenericRecord> list, String fileName) {
            Flowable<GenericRecord> records = Flowable.fromIterable(list);
            client.writeAllData(fileName, schema, records, "").blockingAwait();
        }
    }
}
