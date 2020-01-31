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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SkattTransformXmlToParquet {

    private static final String OUTUT_FOLDER = "output";


    // A simple command line app to generate a parquet from random data
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: java -jar skatt-synthetic-data-to-parquet.jar <schema.avsc> <batchsize> <numbatches> <outfolder> [delete-existing-output=true] \n"
                    + "Example\n"
                    + "skatt-synthetic-data-to-parquet.jar skatt-v0.53.avsc 10000 10 files delete-existing-output=true\n");
            return;
        }

        String avroSchemaFileName = args[0];
        int batchSize = Integer.parseInt(args[1]);
        int numBatches = Integer.parseInt(args[2]);
        String outFolder = args[3];

        if (args.length > 4 && args[4].equals("delete-existing-output=true")) {
            System.out.println("Deleting previous files");
            deleteOldFiles(outFolder);
        }

        createFiles(avroSchemaFileName, batchSize, numBatches, outFolder);
    }

    private static void createFiles(String avroSchemaFileName, int batchSize, int numBatches, String outFolder) {
        int totalItemCount = batchSize * numBatches;
        Schema schema = getSchema(avroSchemaFileName);
        SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);
        String filePrefix = avroSchemaFileName.split("\\.avsc")[0];

        long startTime = System.currentTimeMillis();
        int fileNum = 0;
        List<GenericRecord> list = new ArrayList<>(batchSize);
        long totalTime = 0;

        FieldChildGenerator fieldHandler = new FieldChildGenerator();
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, totalItemCount, fieldHandler);
        for (DataElement dataElement : generateSyntheticData) {
            GenericRecord record = SchemaAwareElement.toRecord(dataElement, schemaBuddy);
            list.add(record);
            if (list.size() == batchSize) {
                System.out.printf("Took %dms to parse %d items%n", System.currentTimeMillis() - startTime, list.size());

                ParquetFileCreator parquetFileCreator = new ParquetFileCreator(OUTUT_FOLDER, outFolder);
                parquetFileCreator.save(schema, list, String.format("%s-%d-%04d.parquet", filePrefix, batchSize, fileNum));
                long time = System.currentTimeMillis() - startTime;
                System.out.printf("Took %dms to parse and create parquet file %d items%n", time, list.size());
                fileNum++;
                list.clear();
                totalTime += time;
                startTime = System.currentTimeMillis();

                System.out.printf("Processed file %d of %d%n", fileNum, numBatches);
            }
        }
        System.out.printf("Took %dms to parse and create parquet files %d items%n", totalTime, totalItemCount);
    }

    static Schema getSchema(String avroJsonSchemaFile) {
        try {
            return new Schema.Parser().parse(new File(avroJsonSchemaFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void deleteOldFiles(String parquetFilesFolder) {
        try {
            File directory = new File(OUTUT_FOLDER + "/" + parquetFilesFolder);
            if (directory.exists()) {
                System.out.println("Deleting " + directory.toString());
                FileUtils.deleteDirectory(directory);
            } else {
                System.out.println("No previous data in " + directory.toString());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
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
