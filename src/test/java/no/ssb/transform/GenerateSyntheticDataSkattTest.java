package no.ssb.transform;


import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaAwareElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import no.ssb.avro.generate.GenerateSyntheticData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;


class GenerateSyntheticDataSkattTest {

    Schema schema = SkattTransformXmlToParquet.getSchema("skatt-v0.59.avsc");

    @Test
    void testPrintSchema() {
        System.out.println(SchemaBuddy.parse(schema).toString(true));
    }

    @Test
    void test() {
        SkattFieldInterceptor skattFieldInterceptor = new SkattFieldInterceptor();

        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 1, skattFieldInterceptor);

//        List<String> results = new ArrayList<>();
        for (DataElement element : generateSyntheticData) {
//            DataElement id = element.findChildByName("personidentifikator");
//            System.out.println(id);
//            DataElement inntektsaar = element.findChildByName("inntektsaar");
//            System.out.println(inntektsaar);
//            DataElement skjermet = element.findChildByName("skjermet");
//            System.out.println(skjermet);
            System.out.println(element.toString(true));
            GenericRecord genericRecord = SchemaAwareElement.toRecord(element, generateSyntheticData.getSchemaBuddy());
            System.out.println(genericRecord);
        }

    }
}