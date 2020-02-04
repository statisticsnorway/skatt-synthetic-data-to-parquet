package no.ssb.transform;

import no.ssb.avro.convert.core.SchemaBuddy;
import no.ssb.avro.generate.FieldInterceptor;
import no.ssb.avro.generate.GeneratedField;

public class SkattFieldInterceptor extends FieldInterceptor {

    long persIdNumber = 1_000_000_000;
    long persIdNumberBarn = 2_000_000_000;

    @Override
    protected GeneratedField handleField(SchemaBuddy schema, int rowNum, int arrayElementNum) {
        switch (schema.getName()) {
            case "personidentifikator":
                if (schema.getParent().getName().equals("barnSomGirRettTilForeldrefradrag")) {
                    long arrayPersIdNumberBarn = persIdNumber * (arrayElementNum + 2);
                    return GeneratedField.fromLong(arrayPersIdNumberBarn + rowNum);
                }
                return GeneratedField.fromLong(persIdNumber + rowNum);
            case "navn":
                if (schema.getParent().getName().equals("barnSomGirRettTilForeldrefradrag")) {
                    return GeneratedField.fromString("barn " + (arrayElementNum + 1));
                }
                break;
            case "barnSomGirRettTilForeldrefradrag":
                return GeneratedField.fromLong(persIdNumberBarn + rowNum);
            case "inntektsaar":
                if ((rowNum % 10 == 0)) return GeneratedField.fromString("2018");
                return GeneratedField.fromString("2019");
            case "skjermet":
                if ((rowNum % 10 == 0) && random.nextBoolean()) return GeneratedField.fromString("true");
                return GeneratedField.fromString("false");
            case "landkode":
                if ((rowNum % 1000 == 0)) return GeneratedField.missingStatus();
                if ((rowNum % 10 == 0)) return GeneratedField.fromString("DK");
                return GeneratedField.fromString("NO");
            case "andelAvFribeloep":
                return createRandom(100);
            default:
                return GeneratedField.shouldBeGenerated();
        }
        return GeneratedField.shouldBeGenerated();
    }

    @Override
    public int getChildCount(int rowNum) {
        return random.nextInt(5);
    }
}
