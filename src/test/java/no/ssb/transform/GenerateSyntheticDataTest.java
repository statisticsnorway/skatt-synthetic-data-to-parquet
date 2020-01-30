package no.ssb.transform;


import no.ssb.avro.convert.core.DataElement;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;


class GenerateSyntheticDataTest {

    Schema schema = SkattTransformXmlToParquet.getSchema("skatt-v0.53.avsc");

    @Test
    void test() {
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema);
        generateSyntheticData.printSchema();
    }

    @Test
    void test2() {
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema);
        DataElement element = generateSyntheticData.parse();

        System.out.println(element.toString(true));
    }

    @Test
    void test3() {
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema);

        for (DataElement element : generateSyntheticData) {
            System.out.println(element.toString(false));
            System.out.println(element.findChildByName("personidentifikator").toString());
        }
    }
}