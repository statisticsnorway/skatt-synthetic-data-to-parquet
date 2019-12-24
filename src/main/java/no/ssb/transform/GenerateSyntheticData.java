package no.ssb.transform;

import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import org.apache.avro.Schema;

import java.util.Iterator;
import java.util.Random;

public class GenerateSyntheticData implements Iterable<DataElement> {
    private Integer count = 0;
    private final Random random = new Random(0);
    long newPersIdNumber = 1_000_000_000;

    private final SchemaBuddy schemaBuddy;

    public GenerateSyntheticData(Schema schema) {
        schemaBuddy = SchemaBuddy.parse(schema);
    }

    void printSchema() {
        String toString = schemaBuddy.toString(true);
        System.out.println(toString);
    }

    DataElement parse() {
        DataElement root = new DataElement(schemaBuddy.getName());
        return parse(root, schemaBuddy);
    }

    DataElement parse(DataElement dataElement, SchemaBuddy schemaBuddy) {
        for (SchemaBuddy childSchema : schemaBuddy.getChildren()) {
            if (childSchema.isArrayType()) {
                parse(dataElement, childSchema);
                continue;
            }
            DataElement childElement = new DataElement(childSchema.getName());
            dataElement.addChild(childElement);
            if (childSchema.isSimpleType()) {
                childElement.setValue(getData(childSchema));
            } else {
                parse(childElement, childSchema);
            }
        }
        return dataElement;
    }

    String insertData(SchemaBuddy schemaBuddy) {
        if (schemaBuddy.getName().equals("personidentifikator")) {
            return Long.toString(newPersIdNumber);
        }
        return null;
    }

    String getData(SchemaBuddy schema) {
        String data = insertData(schema);
        if (data != null) {
            return data;
        }

        assert schema.isSimpleType();
        if (schema.getType() == Schema.Type.STRING) {
            return schema.getName() + "_" + count.toString();
        }
        return Integer.toString(random.nextInt(100_000));
    }

    @Override
    public Iterator<DataElement> iterator() {

        return new Iterator<DataElement>() {
            @Override
            public boolean hasNext() {
                return count < 5000;
            }

            @Override
            public DataElement next() {
                count++;
                newPersIdNumber++;
                return parse();
            }
        };
    }
}
