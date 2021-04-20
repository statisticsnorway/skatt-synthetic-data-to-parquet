package no.ssb.transform;


import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.generate.FieldInterceptor;
import no.ssb.avro.generate.GenerateSyntheticData;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

class GenerateSyntheticDataTest {

    @Test
    void printSchemaWithValues() {
        Schema schema = TransformXmlToParquet.getSchema("sample.avsc");
        FieldInterceptor fieldInterceptor = new SampleFieldInterceptor();

        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 1, fieldInterceptor, 0);

        StringBuilder sb = new StringBuilder();
        for (DataElement element : generateSyntheticData) {
            sb.append(element.toString(true));
        }

        String expected = "spark_schema value:null\n" +
                " |-- fnr value:1000000001\n" +
                " |-- count value:10" +
                "\n";

        assert expected.equals(sb.toString());
    }
}