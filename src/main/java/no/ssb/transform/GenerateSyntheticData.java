package no.ssb.transform;

import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import org.apache.avro.Schema;

import java.util.Iterator;
import java.util.Random;

public class GenerateSyntheticData implements Iterable<DataElement> {
    private static final int DEFAULT_CHILD_COUNT = 2;

    public interface FieldHandler {
        String field(Schema.Type type, String field, String value);
    }

    public interface ChildCountHandler {
        int getChildCount();
    }

    public interface FieldChildHandler extends FieldHandler, ChildCountHandler {

    }

    private Integer count = 0;

    private final Random random = new Random(0);
    private final SchemaBuddy schemaBuddy;
    private final int numToGenerate;
    private final FieldHandler fieldHandler;
    private final ChildCountHandler childCountHandler;

    public GenerateSyntheticData(Schema schema, int numToGenerate, FieldHandler fieldHandler) {
        this(schema, numToGenerate, fieldHandler, () -> DEFAULT_CHILD_COUNT);
    }

    public GenerateSyntheticData(Schema schema, int numToGenerate, FieldChildHandler fieldChildHandler) {
        this(schema, numToGenerate, fieldChildHandler, fieldChildHandler);
    }

    public GenerateSyntheticData(Schema schema, int numToGenerate, FieldHandler fieldHandler, ChildCountHandler childCountHandler) {
        schemaBuddy = SchemaBuddy.parse(schema);
        this.numToGenerate = numToGenerate;
        this.childCountHandler = childCountHandler;
        long persIdNumber = 1_000_000_000;
        if (fieldHandler == null) {
            fieldHandler = (type, field, value) -> {
                if (type == Schema.Type.STRING && field.equals("personidentifikator")) {
                    return Long.toString(persIdNumber + getCount());
                }
                return value;
            };
        }
        this.fieldHandler = fieldHandler;
    }

    public Integer getCount() {
        return count;
    }

    void printSchema() {
        String toString = schemaBuddy.toString(true);
        System.out.println(toString);
    }

    DataElement parse() {
        DataElement root = new DataElement(schemaBuddy.getName());
        return parse(root, schemaBuddy, 0);
    }

    DataElement parse(DataElement dataElement, SchemaBuddy schemaBuddy, int arrayElementCount) {
        for (SchemaBuddy childSchema : schemaBuddy.getChildren()) {
            if (childSchema.isArrayType()) {
                for (int i = 0; i < childCountHandler.getChildCount(); i++) {
                    parse(dataElement, childSchema, i);
                    arrayElementCount++;
                }
                continue;
            }

            DataElement childElement = new DataElement(childSchema.getName());
            dataElement.addChild(childElement);
            if (childSchema.isSimpleType()) {
                childElement.setValue(getData(childSchema, arrayElementCount));
            } else {
                parse(childElement, childSchema, arrayElementCount);
            }
        }
        return dataElement;
    }

    String intercept(SchemaBuddy schemaBuddy, String defaultValue) {
        return fieldHandler.field(schemaBuddy.getType(), schemaBuddy.getName(), defaultValue);
    }

    String getData(SchemaBuddy schema, int arrayElementCount) {
        String generatedData = generatedData(schema, arrayElementCount);
        return intercept(schema, generatedData);
    }

    private String generatedData(SchemaBuddy schema, int arrayElementCount) {
        assert schema.isSimpleType();
        if (schema.getType() == Schema.Type.STRING) {
            return schema.getName() + "_" + count + "_" + arrayElementCount;
        }
        return Integer.toString(random.nextInt(100_000));
    }

    @Override
    public Iterator<DataElement> iterator() {

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return count < numToGenerate;
            }

            @Override
            public DataElement next() {
                count++;
                return parse();
            }
        };
    }
}
