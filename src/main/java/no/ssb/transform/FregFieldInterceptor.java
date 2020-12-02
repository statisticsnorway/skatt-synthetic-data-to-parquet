package no.ssb.transform;

import no.ssb.avro.convert.core.SchemaBuddy;
import no.ssb.avro.generate.FieldInterceptor;
import no.ssb.avro.generate.GeneratedField;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FregFieldInterceptor extends FieldInterceptor {

    private static final long PERS_ID_NUMBER = 1_000_000_000;

    private final LocalDateTime start = LocalDateTime.of(2020, 1, 1, 0, 0);
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
    private final DateTimeFormatter formatterYear = DateTimeFormatter.ofPattern("yyyy");

    @Override
    protected GeneratedField handleField(SchemaBuddy schema, int rowNum, int arrayElementNum) {
        switch (schema.getName()) {
            case "folkeregisteridentifikator":
            case "rettIdentitetVedIdentifikasjonsnummer":
            case "foedselsEllerDNummer":
            case "relatertPerson":
            case "relatertVedSivilstand":
            case "ansvarlig":
            case "ansvarssubjekt":
                return GeneratedField.fromLong(PERS_ID_NUMBER + rowNum);

            case "organisasjonsnummer": //  (9 siffer)
                return GeneratedField.fromLong(100_000_000 + random.nextInt(899_999_999));

            case "bruksenhetsnummer": //  (H0101)
                return GeneratedField.fromString("H0101");

            case "kommunenummer": //  (4 tilfeldige nummer)
            case "kommune":
            case "postnummer":
            case "bostedskommune":
                return createRandom(9999);

            case "grunnkrets": //  (5 tilfeldige nummer)
                return createRandom(9999);

            case "adresselinje": //  (5 siffer starter på 0000)
                return createRandom(99999);

            case "adressekode": //  (start på 0 og ett tilfeldig tall oppover ikke over 99999)
                return createRandom(99999);

            case "husnummer": //  (start på 0 og ett tilfeldig tall oppover ikke over 9999)
            case "gaardsnummer":
            case "bruksnummer":
            case "festenummer":
            case "undernummer":
                return createRandom(9999);

            case "stemmekrets": //  (start på 0 og ett tilfeldig tall oppover ikke over 999)
            case "skolekrets":
            case "kirkekrets":
                return createRandom(999);

            case "startdatoForKontrakt":
            case "sluttdatoForKontrakt":
            case "foedselsdato":
            case "sivilstandsdato":
            case "doedsdato":
            case "flyttedato":
            case "ervervsdato":
            case "datoForBibehold":
            case "oppholdFra":
            case "oppholdTil":
            case "attestutstedelsesdato":
                return GeneratedField.fromString(start.plusDays(rowNum).format(formatter));

                case "foedselsaar": // (men bare yyyy)":
                return GeneratedField.fromString(start.plusDays(rowNum).format(formatterYear));
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
