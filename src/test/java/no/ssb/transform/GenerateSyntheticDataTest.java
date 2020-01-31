package no.ssb.transform;


import no.ssb.avro.convert.core.DataElement;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

class GenerateSyntheticDataTest {

    Schema schema = SkattTransformXmlToParquet.getSchema("skatt-v0.53.avsc");

    @Test
    void testFieldHandler() {
        FieldChildGenerator fieldHandler = new FieldChildGenerator();
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 1, fieldHandler, fieldHandler);
        DataElement element = generateSyntheticData.parse();

        System.out.println(element.toString(true));
    }
}
