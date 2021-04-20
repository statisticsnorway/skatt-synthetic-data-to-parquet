package no.ssb.transform;

import no.ssb.avro.generate.FieldInterceptor;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;

public class TransformXmlToParquet {

    private static final String OUTUT_FOLDER = "output";

    // A simple command line app to generate a parquet from random data
    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Usage: java -jar skatt-synthetic-data-to-parquet.jar <interceptor> <schema.avsc> <batchsize> <numbatches> <outfolder> [delete-existing-output=true] \n");
            return;
        }

        String interspetor = args[0];
        String avroSchemaFileName = args[1];
        int batchSize = Integer.parseInt(args[2]);
        int numBatches = Integer.parseInt(args[3]);
        int rowGroupSize = Integer.parseInt(args[4]);
        String outFolder = args[5];
        int startBatch = Integer.parseInt(args[6]);

        if (args.length > 7 && args[7].equals("delete-existing-output=true")) {
            deleteOldFiles(outFolder);
        }
        FieldInterceptor fieldInterceptor;
        try {
            Constructor<FieldInterceptor> constructor = (Constructor)Class.forName(interspetor).getConstructor();
            fieldInterceptor = constructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ParquetFileHandler.Builder builder = new ParquetFileHandler.Builder();
        ParquetFileHandler fileHandler = builder.addAvroSchemaFileName(avroSchemaFileName)
                .addInterceptor(fieldInterceptor)
                .addAvroSchemaFileName(avroSchemaFileName)
                .addBatchSize(batchSize)
                .addNumBatches(numBatches)
                .addStartBatch(startBatch)
                .addRowGroupSize(rowGroupSize)
                .addFolder(outFolder)
                .build();

        fileHandler.createFiles();
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
}
