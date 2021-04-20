package no.ssb.transform;

import no.ssb.avro.convert.core.SchemaBuddy;
import no.ssb.avro.generate.FieldInterceptor;
import no.ssb.avro.generate.GeneratedField;

public class SampleFieldInterceptor extends FieldInterceptor {

    private static final long PERS_ID_NUMBER = 1_000_000_000;

    @Override
    protected GeneratedField handleField(SchemaBuddy schema, int rowNum, int arrayElementNum) {
        switch (schema.getName()) {
            case "fnr":
                return GeneratedField.fromLong(PERS_ID_NUMBER + rowNum);
            case "count":
                return GeneratedField.fromLong(10);
            default:
                return GeneratedField.shouldBeGenerated();
        }
    }

    @Override
    public int getChildCount(int rowNum) {
        return 1;
    }

    @Override
    public boolean skipRecord(SchemaBuddy schema, int rowNum, int level) {
        return false;
    }

    @Override
    public boolean skipField(SchemaBuddy schema, int rowNum, int level) {
        return false;
    }
}
