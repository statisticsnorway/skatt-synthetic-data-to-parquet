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
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SkattTransformXmlToParquet {

    // A simple command line app to generate a parquet from random data
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        SkattSchema sourceSchema = new SkattSchema();

        SchemaBuddy schemaBuddy = SchemaBuddy.parse(sourceSchema.getRootSchema());
        List<GenericRecord> list = new ArrayList<>(10000);
        int cnt = 0;
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(sourceSchema.getRootSchema());
        for (DataElement dataElement : generateSyntheticData) {
            GenericRecord record = SchemaAwareElement.toRecord(dataElement, schemaBuddy);
            list.add(record);
            cnt++;
        }

        System.out.printf("Took %dms to parse %d items", System.currentTimeMillis() - startTime, cnt);

        ParquetFileCreator parquetFileCreator = new ParquetFileCreator("data", "files");

        deleteOldFiles();

        parquetFileCreator.save(sourceSchema.getRootSchema(), list, "skatt-2-levels-v0.29.parquet");
        System.out.printf("Took %dms to parse and create parquet file %d items", System.currentTimeMillis() - startTime, cnt);
    }

    private static void deleteOldFiles() {
        try {
            File directory = new File("data/files");
            FileUtils.deleteDirectory(directory);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
