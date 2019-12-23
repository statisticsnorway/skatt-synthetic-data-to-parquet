package no.ssb.transform;

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;

public class SkattSchema {

    public Schema getRootSchema() {

        try {
            return new Schema.Parser().parse(new File("skatt-v0.29.avsc"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
