package no.ssb.transform;

import no.ssb.avro.convert.core.SchemaBuddy;
import no.ssb.avro.generate.FieldInterceptor;

public class SkattFieldInterceptor extends FieldInterceptor {

    long persIdNumber = 1_000_000_000;

    @Override
    public String field(SchemaBuddy schema, int rowNum, int arrayElementNum) {
        if (schema.getName().equals("personidentifikator")) {
            return Long.toString(persIdNumber + rowNum);
        }
        return generatedData(schema, rowNum, arrayElementNum);
    }

    @Override
    public int getChildCount(int rowNum) {
        return 2;
    }
}
