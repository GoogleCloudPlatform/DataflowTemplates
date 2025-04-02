/*
 * Copyright (C) 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.bigtable;

import static com.google.cloud.teleport.bigtable.BigtableToAvro.BigtableToAvroFn;
import static com.google.cloud.teleport.bigtable.TestUtils.addAvroCell;
import static com.google.cloud.teleport.bigtable.TestUtils.createAvroRow;
import static com.google.cloud.teleport.bigtable.TestUtils.createBigtableRow;
import static com.google.cloud.teleport.bigtable.TestUtils.upsertBigtableCell;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.teleport.bigtable.BigtableToAvro.Options;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for BigtableToAvro with timestamp filtering.
 */
@RunWith(JUnit4.class)
public final class BigtableToAvroTimestampTest {

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    // Column family and qualifier used in test data
    private static final String COLUMN_FAMILY = "family1";
    private static final String COLUMN_QUALIFIER = "column1";

    // Timestamps for data (in microseconds)
    private static final long EARLY_TS = new DateTime(2024, 10, 20, 10, 0, 0, DateTimeZone.UTC).getMillis() * 1000;
    private static final long MID_TS = new DateTime(2024, 10, 25, 10, 0, 0, DateTimeZone.UTC).getMillis() * 1000;
    private static final long LATE_TS = new DateTime(2024, 10, 30, 10, 0, 0, DateTimeZone.UTC).getMillis() * 1000;

    // Filter timestamp range
    private static final String START_TIMESTAMP = "2024-10-23T00:00:00.00Z";
    private static final String END_TIMESTAMP = "2024-10-27T00:00:00.00Z";

    private static final long START_TS_MICROS =
            new DateTime(2024, 10, 23, 0, 0, 0, DateTimeZone.UTC).getMillis() * 1000;
    private static final long END_TS_MICROS =
            new DateTime(2024, 10, 27, 0, 0, 0, DateTimeZone.UTC).getMillis() * 1000;

    private Options options;
    private ValueProvider<String> emptyValueProvider;

    @Before
    public void setUp() {
        options = mock(Options.class);
        emptyValueProvider = StaticValueProvider.of("");

        // Set up filter
        when(options.getStartTimestamp()).thenReturn(StaticValueProvider.of(START_TIMESTAMP));
        when(options.getEndTimestamp()).thenReturn(StaticValueProvider.of(END_TIMESTAMP));
    }

    @Test
    public void testBigtableToAvroWithTimestampFilter() {
        // Create test data with different timestamps
        Row earlyRow = createBigtableRow("row1");
        earlyRow = upsertBigtableCell(earlyRow, COLUMN_FAMILY, COLUMN_QUALIFIER, EARLY_TS, "early-value");

        Row midRow = createBigtableRow("row2");
        midRow = upsertBigtableCell(midRow, COLUMN_FAMILY, COLUMN_QUALIFIER, MID_TS, "mid-value");

        Row lateRow = createBigtableRow("row3");
        lateRow = upsertBigtableCell(lateRow, COLUMN_FAMILY, COLUMN_QUALIFIER, LATE_TS, "late-value");

        final List<Row> bigtableRows = ImmutableList.of(earlyRow, midRow, lateRow);

        // Create expected output (only the row with timestamp in range)
        BigtableRow expectedAvroRow = createAvroRow("row2");
        addAvroCell(expectedAvroRow, COLUMN_FAMILY, COLUMN_QUALIFIER, MID_TS, "mid-value");
        final List<BigtableRow> expectedAvroRows = ImmutableList.of(expectedAvroRow);

        // Simulate the filter by applying TimestampFilterPredicate
        PCollection<Row> filteredRows =
                pipeline
                        .apply("Create", Create.of(bigtableRows))
                        .apply("Apply Timestamp Filter", Filter.by(new TimestampFilterPredicate()));

        // Convert filtered rows to Avro format
        PCollection<BigtableRow> avroRows =
                filteredRows.apply("Transform to Avro", MapElements.via(new BigtableToAvroFn()));

        // Assert that only the row with timestamp in range is present
        PAssert.that(avroRows).containsInAnyOrder(expectedAvroRows);

        pipeline.run();
    }

    /**
     * Test utility to simulate the timestamp filter behavior.
     */
    private static class TimestampFilterPredicate implements SerializableFunction<Row, Boolean> {
        private static final long START_TS_MICROS =
                new DateTime(2024, 10, 23, 0, 0, 0, DateTimeZone.UTC).getMillis() * 1000;
        private static final long END_TS_MICROS =
                new DateTime(2024, 10, 27, 0, 0, 0, DateTimeZone.UTC).getMillis() * 1000;

        @Override
        public Boolean apply(Row row) {
            // Check each cell's timestamp to determine if the row should be included
            for (com.google.bigtable.v2.Family family : row.getFamiliesList()) {
                for (com.google.bigtable.v2.Column column : family.getColumnsList()) {
                    for (com.google.bigtable.v2.Cell cell : column.getCellsList()) {
                        long timestamp = cell.getTimestampMicros();
                        if (timestamp >= START_TS_MICROS && timestamp < END_TS_MICROS) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }

    /**
     * Tests timestamp filter creation with valid start and end timestamps.
     */
    @Test
    public void testFilterCreation_BothTimestamps() {
        // Setup
        ValueProvider<String> startTimestamp = StaticValueProvider.of("2024-10-27T10:15:10.00Z");
        ValueProvider<String> endTimestamp = StaticValueProvider.of("2024-10-27T10:15:30.00Z");

        when(options.getStartTimestamp()).thenReturn(startTimestamp);
        when(options.getEndTimestamp()).thenReturn(endTimestamp);

        // Calculate expected microseconds
        DateTime startDateTime = new DateTime(2024, 10, 27, 10, 15, 10, DateTimeZone.UTC);
        DateTime endDateTime = new DateTime(2024, 10, 27, 10, 15, 30, DateTimeZone.UTC);
        long expectedStartMicros = startDateTime.getMillis() * 1000;
        long expectedEndMicros = endDateTime.getMillis() * 1000;

        // Use the new method to create the filterProvider
        ValueProvider<RowFilter> filterProvider = BigtableToAvro.createRowFilterProvider(options);
        RowFilter filter = filterProvider.get();

        // Verify filter content
        String filterString = filter.toString();
        assertTrue(filterString.contains("timestamp_range_filter"));
        assertTrue(filterString.contains("start_timestamp_micros: " + expectedStartMicros));
        assertTrue(filterString.contains("end_timestamp_micros: " + expectedEndMicros));
    }

    /**
     * Tests filter creation when no timestamps are provided.
     */
    @Test
    public void testFilterCreation_NoTimestamps() {
        // Setup
        when(options.getStartTimestamp()).thenReturn(emptyValueProvider);
        when(options.getEndTimestamp()).thenReturn(emptyValueProvider);

        // Use the new method to create the filterProvider
        ValueProvider<RowFilter> filterProvider = BigtableToAvro.createRowFilterProvider(options);

        // Verify that no filter is created when no timestamps are provided
        assertNull(filterProvider.get());
    }

    /**
     * Tests that an exception is thrown when only start timestamp is provided.
     */
    @Test
    public void testFilterCreation_OnlyStartTimestamp() {
        // Setup
        ValueProvider<String> startTimestamp = StaticValueProvider.of("2024-10-27T10:15:10.00Z");

        when(options.getStartTimestamp()).thenReturn(startTimestamp);
        when(options.getEndTimestamp()).thenReturn(emptyValueProvider);

        // Use the new method to create the filterProvider
        ValueProvider<RowFilter> filterProvider = BigtableToAvro.createRowFilterProvider(options);

        // Verify that an exception is thrown when only start timestamp is provided
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> filterProvider.get());

        assertEquals(
                "Both startTimestamp and endTimestamp must be provided together, or neither should be provided.",
                exception.getMessage());
    }

    /**
     * Tests that an exception is thrown when only end timestamp is provided.
     */
    @Test
    public void testFilterCreation_OnlyEndTimestamp() {
        // Setup
        ValueProvider<String> endTimestamp = StaticValueProvider.of("2024-10-27T10:15:30.00Z");

        when(options.getStartTimestamp()).thenReturn(emptyValueProvider);
        when(options.getEndTimestamp()).thenReturn(endTimestamp);

        // Use the new method to create the filterProvider
        ValueProvider<RowFilter> filterProvider = BigtableToAvro.createRowFilterProvider(options);

        // Verify that an exception is thrown when only end timestamp is provided
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> filterProvider.get());

        assertEquals(
                "Both startTimestamp and endTimestamp must be provided together, or neither should be provided.",
                exception.getMessage());
    }

    /**
     * Tests that an exception is thrown when an invalid timestamp format is provided.
     */
    @Test
    public void testFilterCreation_InvalidTimestampFormat() {
        // Setup for invalid date format
        ValueProvider<String> invalidStartTimestamp = StaticValueProvider.of("2024/10/27 10:15:10");
        ValueProvider<String> validEndTimestamp = StaticValueProvider.of("2024-10-27T10:15:30.00Z");

        when(options.getStartTimestamp()).thenReturn(invalidStartTimestamp);
        when(options.getEndTimestamp()).thenReturn(validEndTimestamp);

        // Use the new method to create the filterProvider
        ValueProvider<RowFilter> filterProvider = BigtableToAvro.createRowFilterProvider(options);

        // Verify that an exception is thrown for invalid date format
        assertThrows(IllegalArgumentException.class, () -> filterProvider.get());
    }
}