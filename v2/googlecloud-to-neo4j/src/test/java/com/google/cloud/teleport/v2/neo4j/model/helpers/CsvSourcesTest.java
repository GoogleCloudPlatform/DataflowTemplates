package com.google.cloud.teleport.v2.neo4j.model.helpers;

import com.google.cloud.teleport.v2.neo4j.model.sources.TextFormat;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class CsvSourcesTest {

    @Test
    public void convertsTextFormatToCsvFormat() {
        assertThat(CsvSources.toCsvFormat(TextFormat.EXCEL)).isEqualTo(CSVFormat.EXCEL.withNullString(""));
        assertThat(CsvSources.toCsvFormat(TextFormat.INFORMIX)).isEqualTo(CSVFormat.INFORMIX_UNLOAD_CSV.withNullString(""));
        assertThat(CsvSources.toCsvFormat(TextFormat.MONGO)).isEqualTo(CSVFormat.MONGODB_CSV.withNullString(""));
        assertThat(CsvSources.toCsvFormat(TextFormat.MONGO_TSV)).isEqualTo(CSVFormat.MONGODB_TSV.withNullString(""));
        assertThat(CsvSources.toCsvFormat(TextFormat.MYSQL)).isEqualTo(CSVFormat.MYSQL.withNullString(""));
        assertThat(CsvSources.toCsvFormat(TextFormat.ORACLE)).isEqualTo(CSVFormat.ORACLE.withNullString(""));
        assertThat(CsvSources.toCsvFormat(TextFormat.POSTGRES)).isEqualTo(CSVFormat.POSTGRESQL_TEXT.withNullString(""));
        assertThat(CsvSources.toCsvFormat(TextFormat.POSTGRESQL_CSV)).isEqualTo(CSVFormat.POSTGRESQL_CSV.withNullString(""));
        assertThat(CsvSources.toCsvFormat(TextFormat.RFC4180)).isEqualTo(CSVFormat.RFC4180.withNullString(""));
        assertThat(CsvSources.toCsvFormat(TextFormat.DEFAULT)).isEqualTo(CSVFormat.DEFAULT.withNullString(""));
    }

}
