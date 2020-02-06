package no.ssb.transform;

import no.ssb.avro.convert.core.SchemaBuddy;
import no.ssb.avro.generate.FieldInterceptor;
import no.ssb.avro.generate.GeneratedField;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class SkattFieldInterceptor extends FieldInterceptor {

    private final long persIdNumber = 1_000_000_000;
    private final long persIdNumberBarn = 2_000_000_000;
    private final LocalDateTime start = LocalDateTime.of(2019, 1, 1, 0, 0);
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

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
            case "registreringsdato":
                return GeneratedField.fromString(start.plusDays(rowNum).format(formatter));
            case "pensjonstype":
                return getPensionType(schema, rowNum, arrayElementNum);
            case "pensjonsordningstype":
                return getPensionSchemeType(schema, rowNum, arrayElementNum);
            default:
                return GeneratedField.shouldBeGenerated();
        }
        return GeneratedField.shouldBeGenerated();
    }

    GeneratedField getPensionType(SchemaBuddy schema, int rowNum, int arrayElementNum) {
        final List<String> personTypes = List.of(
                "Ektefelletillegg",
                "ufoeretrygdFraFolketrygden",
                "ufoereytelseFraIPAOgIPS",
                "ufoereytelseFraUtlandet",
                "annenPensjonFraUtlandet",
                "supplerendeStoenad",
                "alderspensjonFraIPAOgIPS",
                "pensjonFraEOESTilsvarendeNorskAlderspensjonEllerAFP"
        );

        if (!schema.getParent().getName().equals("pensjonsinntekt")) {
            return GeneratedField.shouldBeGenerated();
        }
        String personType = personTypes.get(random.nextInt(personTypes.size()));
        return GeneratedField.fromString(personType);
    }

    GeneratedField getPensionSchemeType(SchemaBuddy schema, int rowNum, int arrayElementNum) {
        final List<String> pensionSchemeTypes = List.of(
                "individuellPensjonsavtale",
                "skattefavorisertIndividuellSparingTilPensjon",
                "pensjonsavtale"
        );

        if (!schema.getParent().getName().equals("individuellPensjonsordning")) {
            return GeneratedField.shouldBeGenerated();
        }
        String pensionSchemeType = pensionSchemeTypes.get(random.nextInt(pensionSchemeTypes.size()));
        return GeneratedField.fromString(pensionSchemeType);
    }

    @Override
    public int getChildCount(int rowNum) {
        return random.nextInt(5);
    }

    @Override
    public boolean skipRecord(SchemaBuddy schema, int rowNum, int level) {
        return random.nextBoolean();
    }

    @Override
    public boolean skipField(SchemaBuddy schema, int rowNum, int level) {
        return random.nextBoolean();
    }
}
