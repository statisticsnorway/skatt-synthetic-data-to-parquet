package no.ssb.transform;


import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaAwareElement;
import no.ssb.avro.generate.GenerateSyntheticData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;


class GenerateSyntheticDataSkattTest {

    Schema schema = SkattTransformXmlToParquet.getSchema("skatt-v0.53.avsc");

    @Test
    void test() {
        SkattFieldInterceptor skattFieldInterceptor = new SkattFieldInterceptor();

        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 2, skattFieldInterceptor);

        for (DataElement element : generateSyntheticData) {
            System.out.println(element.toString(true));
//            GenericRecord genericRecord = SchemaAwareElement.toRecord(element, generateSyntheticData.getSchemaBuddy());
//            System.out.println(genericRecord);
        }

    }
}