package no.ssb.transform;

import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import org.apache.avro.Schema;

import java.util.Iterator;
import java.util.Random;

public class GenerateSyntheticData implements Iterable<DataElement> {
    private static final int DEFAULT_CHILD_COUNT = 2;
    private RowHandler rowHandler;

    public interface FieldHandler {
        String field(Schema.Type type, String field, String value, int rowNum);
    }

    public interface ChildCountHandler {
        int getChildCount(int rowNum);
    }

    public interface RowHandler {
        int onNewRow(int rowNum);
    }

    public interface FieldChildHandler extends FieldHandler, ChildCountHandler, RowHandler {

    }

    private Integer count = 0;

    private final Random random = new Random(0);
    private final SchemaBuddy schemaBuddy;
    private final int numToGenerate;
    private final FieldHandler fieldHandler;
    private final ChildCountHandler childCountHandler;

    public GenerateSyntheticData(Schema schema, int numToGenerate, FieldHandler fieldHandler) {
        this(schema, numToGenerate, fieldHandler, (i) -> DEFAULT_CHILD_COUNT);
    }

    public GenerateSyntheticData(Schema schema, int numToGenerate, FieldChildHandler fieldChildHandler) {
        this(schema, numToGenerate, fieldChildHandler, fieldChildHandler);
    }

    public GenerateSyntheticData(Schema schema, int numToGenerate, FieldHandler fieldHandler, ChildCountHandler childCountHandler) {
        this(schema, numToGenerate, fieldHandler, childCountHandler, null);
    }

    public GenerateSyntheticData(Schema schema, int numToGenerate, FieldHandler fieldHandler, ChildCountHandler childCountHandler, RowHandler rowHandler) {
        schemaBuddy = SchemaBuddy.parse(schema);
        this.numToGenerate = numToGenerate;
        this.childCountHandler = childCountHandler;
        this.fieldHandler = fieldHandler;
        if (rowHandler == null) {
            rowHandler = rowNum -> count;
        }
        this.rowHandler = rowHandler;
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
                for (int i = 0; i < childCountHandler.getChildCount(0); i++) {
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
        return fieldHandler.field(schemaBuddy.getType(), schemaBuddy.getName(), defaultValue, 0);
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
                rowHandler.onNewRow(count++);
                return parse();
            }
        };
    }
}
