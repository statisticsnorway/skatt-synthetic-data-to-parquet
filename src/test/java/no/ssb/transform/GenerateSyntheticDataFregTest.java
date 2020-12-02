package no.ssb.transform;


import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaAwareElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import no.ssb.avro.generate.GenerateSyntheticData;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;


class GenerateSyntheticDataFregTest {

    Schema schema = SkattTransformXmlToParquet.getSchema("freg-v1.4.avsc");

    @Test
    void testPrintSchema() {
        System.out.println(SchemaBuddy.parse(schema).toString(true));
    }

    @Test
    void testConvertAllToMandatory() {
        SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);

        System.out.println(schemaBuddy.toString(true));
    }

    @Test
    void test() {
        FregFieldInterceptor fregFieldInterceptor = new FregFieldInterceptor();

        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 1, fregFieldInterceptor, 0);


        SchemaBuddy schemaBuddy = SchemaBuddy.parse(schema);
//        System.out.println(schemaBuddy.toString(true));

//        SchemaBuddy personidentifikator = schemaBuddy.getChild("personidentifikator");
//
//        System.out.println(personidentifikator.isOptionalWithCheckOfAllChildren());


//        System.out.println(schema.toString(true));
//        if(true) return;


//        List<String> results = new ArrayList<>();
        for (DataElement element : generateSyntheticData) {

            System.out.println( element.toString(true));

            DataElement id = element.findChildByName("folkeregisteridentifikator");
//            System.out.println(id);
//            DataElement inntektsaar = element.findChildByName("inntektsaar");
//            System.out.println(inntektsaar);
//            DataElement skjermet = element.findChildByName("skjermet");
//            System.out.println(skjermet);
//            System.out.println(element.toString(true));
//            SchemaAwareElement schemaAwareElement = SchemaAwareElement.toSchemaAwareElement(element, schemaBuddy);
//            System.out.println(schemaAwareElement.toString(true));
//            GenericRecord genericRecord = SchemaAwareElement.toRecord(element, generateSyntheticData.getSchemaBuddy());
//            System.out.println(genericRecord);
        }

    }
}