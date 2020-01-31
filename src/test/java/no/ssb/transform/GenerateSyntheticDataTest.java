package no.ssb.transform;


import no.ssb.avro.convert.core.DataElement;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


class GenerateSyntheticDataTest {

    Schema schema = SkattTransformXmlToParquet.getSchema("skatt-v0.53.avsc");
    FieldChildGenerator fieldChildGenerator = new FieldChildGenerator();

    long persIdNumber = 1_000_000_000;
    GenerateSyntheticData.FieldHandler fieldHandler = (type, field, value, number) -> {
        if (field.equals("personidentifikator")) {
            return Long.toString(persIdNumber);
        }
        return value;
    };

    @Test
    void test() {
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 10, null);
        generateSyntheticData.printSchema();
    }

    @Test
    void test2() {
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 1, null);
        DataElement element = generateSyntheticData.parse();

        System.out.println(element.toString(true));
    }

    @Test
    void testFieldHandler() {
        FieldChildGenerator fieldHandler = new FieldChildGenerator();
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 1, fieldHandler, fieldHandler);
        DataElement element = generateSyntheticData.parse();

        System.out.println(element.toString(true));
    }

    @Test
    void test3() {
        AtomicInteger cnt = new AtomicInteger();
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 2, fieldChildGenerator);

        for (DataElement element : generateSyntheticData) {
            System.out.println(element.toString(true));
//            System.out.println(element.findChildByName("konto").toString(true));
        }
    }

    @Test
    void test4() {
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 10, (type, field, value, number) -> {
            System.out.println(type);
            System.out.println(field);
            System.out.println(value);
            return value;
        });

        for (DataElement element : generateSyntheticData) {
//            System.out.println(element.toString(false));
            System.out.println(element.findChildByName("personidentifikator").toString());
        }
    }

    @Test
    void test5() throws IOException {

//        ParquetProvider.Configuration parquetConfiguration = new ParquetProvider.Configuration();
//        parquetConfiguration.setPageSize(8 * 1024 * 1024);
//        parquetConfiguration.setRowGroupSize(64 * 1024 * 1024);
//
//        DataClient.Configuration clientConfiguration = new DataClient.Configuration();
//        clientConfiguration.setLocation("skatt-v0.53.avsc" + "/");
//
//        DataClient dataClient = DataClient.builder()
//                .withParquetProvider(new ParquetProvider(parquetConfiguration))
//                .withBinaryBackend(new LocalBackend("output/"))
//                .withConfiguration(clientConfiguration)
//                .build();
//
//        Flowable<GenericRecord> genericRecordFlowable = dataClient.readData("skatt-v0.53-10-0000.parquet", schema, "", null);
//
//        System.out.println(System.getProperty("java.version"));
//
//        genericRecordFlowable.blockingIterable().forEach(genericRecord -> {
//            System.out.println(genericRecord.get("personidentifikator"));
//        });
//

//        Path path = new Path("output/skatt-v0.53.avsc/skatt-v0.53-10-0000.parquet");
//        boolean exists = new File(path.toString()).exists();
//        System.out.println(exists);
//        Configuration conf = new Configuration(true);
//        InputFile inputFile = HadoopInputFile.fromPath(path, conf);
//        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path).build();
//        GenericRecord nextRecord = reader.read();
//
//        System.out.println(nextRecord.get("personidentifikator"));

    }
}