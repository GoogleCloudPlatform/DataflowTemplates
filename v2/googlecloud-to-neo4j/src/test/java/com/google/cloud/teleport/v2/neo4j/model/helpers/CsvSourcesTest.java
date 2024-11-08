package com.google.cloud.teleport.v2.neo4j.model.helpers;

import com.google.cloud.teleport.v2.neo4j.model.sources.TextFormat;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class CsvSourcesTest {

    @Parameterized.Parameter(0)
    public TextFormat textFormat;

    @Parameterized.Parameter(1)
    public char delimiter;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> testParameters() {
        return Arrays.asList(new Object[][]{
                {TextFormat.EXCEL, ','},
                {TextFormat.INFORMIX, ','},
                {TextFormat.MONGO, ','},
                {TextFormat.MONGO_TSV, '\t'},
                {TextFormat.MYSQL, '\t'},
                {TextFormat.ORACLE, ','},
                {TextFormat.POSTGRES, '\t'},
                {TextFormat.POSTGRESQL_CSV, ','},
                {TextFormat.RFC4180, ','},
                {TextFormat.DEFAULT, ','}
        });
    }

    @Test
    public void shouldParseEmptyColumnsAsNullValues() throws IOException {
        String line = "1" + delimiter;
        CSVFormat csvFormat = CsvSources.toCsvFormat(textFormat);

        try(CSVParser csvParser = CSVParser.parse(line, csvFormat)) {
            List<CSVRecord> csvRecords = csvParser.getRecords();
            assertThat(csvRecords).hasSize(1);

            CSVRecord csvRecord = csvRecords.get(0);
            assertThat(csvRecord).hasSize(2);
            assertThat(csvRecord.get(0)).isEqualTo("1");
            assertThat(csvRecord.get(1)).isEqualTo(null);
        }
    }
}
