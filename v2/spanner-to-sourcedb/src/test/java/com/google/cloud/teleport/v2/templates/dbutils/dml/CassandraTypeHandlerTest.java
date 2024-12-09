package com.google.cloud.teleport.v2.templates.dbutils.dml;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.*;

import static com.google.cloud.teleport.v2.templates.dbutils.dml.CassandraTypeHandler.*;
import static com.google.cloud.teleport.v2.templates.dbutils.dml.CassandraTypeHandler.handleCassandraTimestampType;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class CassandraTypeHandlerTest {

    @Test
    public void convertSpannerValueJsonToBooleanType() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"isAdmin\":\"true\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "isAdmin";
        Boolean convertedValue = handleCassandraBoolType(colKey, newValuesJson);
        assertTrue(convertedValue);
    }

    @Test
    public void convertSpannerValueJsonToBooleanType_False() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"isAdmin\":\"false\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "isAdmin";
        Boolean convertedValue = handleCassandraBoolType(colKey, newValuesJson);
        Assert.assertFalse(convertedValue);
    }

    @Test
    public void convertSpannerValueJsonToFloatType() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"age\":23.5}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "age";
        Float convertedValue = handleCassandraFloatType(colKey, newValuesJson);
        assertEquals(23.5f, convertedValue, 0.01f);
    }

    @Test
    public void convertSpannerValueJsonToDoubleType() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"salary\":100000.75}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "salary";
        Double convertedValue = handleCassandraDoubleType(colKey, newValuesJson);
        assertEquals(100000.75, convertedValue, 0.01);
    }

    @Test
    public void convertSpannerValueJsonToBlobType_FromByteArray() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"data\":\"QUJDQDEyMzQ=\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "data";
        ByteBuffer convertedValue = handleCassandraBlobType(colKey, newValuesJson);
        byte[] expectedBytes = java.util.Base64.getDecoder().decode("QUJDQDEyMzQ=");
        byte[] actualBytes = new byte[convertedValue.remaining()];
        convertedValue.get(actualBytes);
        Assert.assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void testHandleUnsupportedType() {
        String newValuesString = "{\"values\":[true, false]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);

        try {
            handleFloatSetType("values", newValuesJson);
            fail("Expected IllegalArgumentException for unsupported type");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsupported type for column values"));
        }
    }

    @Test
    public void testHandleUnsupportedMapType() {
        String newValuesString = "{\"values\":[{\"key1\":\"value1\"}, {\"key2\":\"value2\"}]}";  // Map (unsupported type)
        JSONObject newValuesJson = new JSONObject(newValuesString);

        try {
            handleFloatSetType("values", newValuesJson);  // This should throw the exception for unsupported type
            fail("Expected IllegalArgumentException for unsupported type");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsupported type for column values"));
        }
    }

    @Test
    public void testHandleUnsupportedListType() {
        String newValuesString = "{\"values\":[[1, 2], [3, 4]]}";  // List of integers (unsupported type)
        JSONObject newValuesJson = new JSONObject(newValuesString);

        try {
            handleFloatSetType("values", newValuesJson);  // This should throw the exception for unsupported type
            fail("Expected IllegalArgumentException for unsupported type");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsupported type for column values"));
        }
    }

    @Test
    public void testHandleUnsupportedBooleanType() {
        String newValuesString = "{\"values\":[true, false]}";  // Boolean values (unsupported)
        JSONObject newValuesJson = new JSONObject(newValuesString);

        try {
            handleFloatSetType("values", newValuesJson);  // This should throw the exception for unsupported type
            fail("Expected IllegalArgumentException for unsupported type");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unsupported type for column values"));
        }
    }

    @Test
    public void convertSpannerValueJsonToBlobType_FromBase64() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"data\":\"QUJDRA==\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "data";
        ByteBuffer convertedValue = handleCassandraBlobType(colKey, newValuesJson);
        byte[] expectedBytes = Base64.getDecoder().decode("QUJDRA==");
        byte[] actualBytes = new byte[convertedValue.remaining()];
        convertedValue.get(actualBytes);
        Assert.assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public void convertSpannerValueJsonToBlobType_EmptyString() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"data\":\"\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "data";
        ByteBuffer convertedValue = handleCassandraBlobType(colKey, newValuesJson);
        Assert.assertNotNull(convertedValue);
        assertEquals(0, convertedValue.remaining());
    }

    @Test(expected = IllegalArgumentException.class)
    public void convertSpannerValueJsonToBlobType_InvalidType() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"data\":12345}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "data";
        handleCassandraBlobType(colKey, newValuesJson);
    }

    @Test
    public void convertSpannerValueJsonToMissingColumn() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "isAdmin";
        Boolean convertedValue = newValuesJson.optBoolean(colKey, false);
        Assert.assertFalse(convertedValue);
    }

    @Test
    public void convertSpannerValueJsonToMissingOrNullColumn() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"isAdmin\":null}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "isAdmin";
        Boolean convertedValue = newValuesJson.isNull(colKey) ? false : newValuesJson.optBoolean(colKey, false);
        Assert.assertFalse(convertedValue);
    }

    @Test
    public void convertSpannerValueJsonToInvalidFloatType() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"age\":\"invalid_value\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "age";
        try {
            handleCassandraFloatType(colKey, newValuesJson);
            fail("Expected a JSONException for invalid float value.");
        } catch (JSONException e) {
            assertTrue(e.getMessage().contains("not a BigDecimal"));
        }
    }

    @Test
    public void convertSpannerValueJsonToInvalidDoubleType() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\", \"salary\":\"invalid_value\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "salary";
        try {
            handleCassandraDoubleType(colKey, newValuesJson);
            fail("Expected a JSONException for invalid double value.");
        } catch (JSONException e) {
            assertTrue(e.getMessage().contains("not a BigDecimal"));
        }
    }

    @Test
    public void convertSpannerValueJsonToBlobType_MissingColumn() {
        String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String colKey = "data";
        ByteBuffer convertedValue = handleCassandraBlobType(colKey, newValuesJson);
        Assert.assertNull(convertedValue);
    }

    @Test
    public void testHandleByteArrayType() {
        String newValuesString = "{\"data\":[\"QUJDRA==\", \"RkZIRg==\"]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        List<ByteBuffer> value = handleByteArrayType("data", newValuesJson);

        List<ByteBuffer> expected = Arrays.asList(
                ByteBuffer.wrap(Base64.getDecoder().decode("QUJDRA==")),
                ByteBuffer.wrap(Base64.getDecoder().decode("RkZIRg=="))
        );
        assertEquals(expected, value);
    }

    @Test
    public void testHandleByteSetType() {
        String newValuesString = "{\"data\":[\"QUJDRA==\", \"RkZIRg==\"]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        Set<ByteBuffer> value = handleByteSetType("data", newValuesJson);

        Set<ByteBuffer> expected = new HashSet<>(Arrays.asList(
                ByteBuffer.wrap(Base64.getDecoder().decode("QUJDRA==")),
                ByteBuffer.wrap(Base64.getDecoder().decode("RkZIRg=="))
        ));
        assertEquals(expected, value);
    }

    @Test
    public void testHandleStringArrayType() {
        String newValuesString = "{\"names\":[\"Alice\", \"Bob\", \"Charlie\"]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        Set<String> value = handleStringArrayType("names", newValuesJson);

        Set<String> expected = new HashSet<>(Arrays.asList("Alice", "Bob", "Charlie"));
        assertEquals(expected, value);
    }

    @Test
    public void testHandleStringSetType() {
        String newValuesString = "{\"names\":[\"Alice\", \"Bob\", \"Alice\", \"Charlie\"]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        List<String> valueList = handleStringSetType("names", newValuesJson);
        Set<String> value = new HashSet<>(valueList);
        Set<String> expected = new HashSet<>(Arrays.asList("Alice", "Bob", "Charlie"));
        assertEquals(expected, value);
    }

    @Test
    public void testHandleBoolSetTypeString() {
        String newValuesString = "{\"flags\":[\"true\", \"false\", \"true\"]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        Set<Boolean> value = handleBoolSetTypeString("flags", newValuesJson);

        Set<Boolean> expected = new HashSet<>(Arrays.asList(true, false));
        assertEquals(expected, value);
    }

    @Test
    public void testHandleFloatArrayType() {
        String newValuesString = "{\"values\":[1.1, 2.2, 3.3]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        List<Float> value = handleFloatArrayType("values", newValuesJson);

        List<Float> expected = Arrays.asList(1.1f, 2.2f, 3.3f);
        assertEquals(expected, value);
    }

    @Test
    public void testHandleFloatSetType() {
        String newValuesString = "{\"values\":[1.1, 2.2, 3.3, 2.2]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        Set<Float> value = handleFloatSetType("values", newValuesJson);

        Set<Float> expected = new HashSet<>(Arrays.asList(1.1f, 2.2f, 3.3f));
        assertEquals(expected, value);
    }

    @Test
    public void testHandleFloatSetType_InvalidString() {
        String newValuesString = "{\"values\":[\"1.1\", \"2.2\", \"abc\"]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        try {
            handleFloatSetType("values", newValuesJson);
            fail("Expected IllegalArgumentException for invalid number format");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid number format for column values"));
        }
    }

    @Test
    public void testHandleFloat64ArrayType() {
        String newValuesString = "{\"values\":[1.1, \"2.2\", 3.3]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        List<Double> value = handleFloat64ArrayType("values", newValuesJson);

        List<Double> expected = Arrays.asList(1.1, 2.2, 3.3);
        assertEquals(expected, value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHandleFloat64ArrayTypeInvalid() {
        String newValuesString = "{\"values\":[\"1.1\", \"abc\", \"3.3\"]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        handleFloat64ArrayType("values", newValuesJson);
    }

    @Test
    public void testHandleDateSetType() {
        String newValuesString = "{\"dates\":[\"2024-12-05\", \"2024-12-06\"]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        Set<Date> value = handleDateSetType("dates", newValuesJson);

        Set<Date> expected = new HashSet<>(Arrays.asList(
                new GregorianCalendar(2024, Calendar.DECEMBER, 5).getTime(),
                new GregorianCalendar(2024, Calendar.DECEMBER, 6).getTime()
        ));
        assertEquals(expected, value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHandleFloat64ArrayType_WithUnsupportedList() {
        String jsonStr = "{\"colName\": [[1, 2, 3], [4, 5, 6]]}";
        JSONObject valuesJson = new JSONObject(jsonStr);
        CassandraTypeHandler.handleFloat64ArrayType("colName", valuesJson);
    }

    @Test
    public void testHandleInt64SetType_ValidLongValues() {
        String newValuesString = "{\"numbers\":[1, 2, 3, 4]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        Set<Long> result = handleInt64SetType("numbers", newValuesJson);
        Set<Long> expected = new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L));
        assertEquals(expected, result);
    }

    @Test
    public void testHandleCassandraIntType_ValidInteger() {
        String newValuesString = "{\"age\":1234}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        Integer result = handleCassandraIntType("age", newValuesJson);
        Integer expected = 1234;
        assertEquals(expected, result);
    }

    @Test
    public void testHandleCassandraBigintType_ValidConversion() {
        String newValuesString = "{\"age\":1234567890123}";  // A valid BigInteger value
        JSONObject newValuesJson = new JSONObject(newValuesString);
        Long result = handleCassandraBigintType("age", newValuesJson);
        Long expected = 1234567890123L;
        assertEquals(expected, result);
    }

    @Test
    public void testByteArrayHandling_ValidByteArray() {
        byte[] inputByteArray = {1, 2, 3, 4, 5};
        Object colValue = inputByteArray;
        byte[] byteArray = null;
        if (colValue instanceof byte[]) {
            byteArray = (byte[]) colValue;
        }
        assertNotNull(byteArray);
        assertArrayEquals(inputByteArray, byteArray);
    }

    @Test
    public void testByteArrayHandling_NotByteArray() {
        String inputValue = "This is a string";
        Object colValue = inputValue;
        byte[] byteArray = null;
        if (colValue instanceof byte[]) {
            byteArray = (byte[]) colValue;
        }
        assertNull(byteArray);
    }

    @Test
    public void testHandleInt64ArrayAsInt32Array() {
        String newValuesString = "{\"values\":[1, 2, 3, 4]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        List<Integer> value = handleInt64ArrayAsInt32Array("values", newValuesJson);

        List<Integer> expected = Arrays.asList(1, 2, 3, 4);
        assertEquals(expected, value);
    }

    @Test
    public void testHandleInt64ArrayAsInt32Set() {
        String newValuesString = "{\"values\":[1, 2, 3, 2]}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        Set<Integer> value = handleInt64ArrayAsInt32Set("values", newValuesJson);

        Set<Integer> expected = new HashSet<>(Arrays.asList(1, 2, 3));
        assertEquals(expected, value);
    }

    @Test
    public void testHandleCassandraUuidTypeNull() {
        String newValuesString = "{\"uuid\":null}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        UUID value = handleCassandraUuidType("uuid", newValuesJson);
        Assert.assertNull(value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHandleCassandraTimestampInvalidFormat() {
        String newValuesString = "{\"createdAt\":\"2024-12-05 10:15:30.123\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        handleCassandraTimestampType("createdAt", newValuesJson);
    }

    @Test
    public void testHandleCassandraTimestampInvalidFormatColNull() {
        String newValuesString = "{\"createdAt\":\"2024-12-05 10:15:30.123\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        handleCassandraTimestampType(null, newValuesJson);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHandleCassandraDateInvalidFormat() {
        String newValuesString = "{\"birthdate\":\"2024/12/05\"}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        handleCassandraDateType("birthdate", newValuesJson);
    }

    @Test
    public void testHandleCassandraTextTypeNull() {
        String newValuesString = "{\"name\":null}";
        JSONObject newValuesJson = new JSONObject(newValuesString);
        String value = handleCassandraTextType("name", newValuesJson);
        Assert.assertNull(value);
    }

    @Test
    public void testHandleBoolArrayType_ValidBooleanStrings() {
        String jsonStr = "{\"colName\": [\"true\", \"false\", \"true\"]}";
        JSONObject valuesJson = new JSONObject(jsonStr);
        List<Boolean> result = CassandraTypeHandler.handleBoolArrayType("colName", valuesJson);
        assertEquals(3, result.size());
        assertTrue(result.get(0));
        assertFalse(result.get(1));
        assertTrue(result.get(2));
    }

    @Test
    public void testHandleBoolArrayType_InvalidBooleanStrings() {
        String jsonStr = "{\"colName\": [\"yes\", \"no\", \"true\"]}";
        JSONObject valuesJson = new JSONObject(jsonStr);
        List<Boolean> result = CassandraTypeHandler.handleBoolArrayType("colName", valuesJson);
        assertEquals(3, result.size());
        assertFalse(result.get(0));
        assertFalse(result.get(1));
        assertTrue(result.get(2));
    }

    @Test
    public void testHandleBoolArrayType_EmptyArray() {
        String jsonStr = "{\"colName\": []}";
        JSONObject valuesJson = new JSONObject(jsonStr);
        List<Boolean> result = CassandraTypeHandler.handleBoolArrayType("colName", valuesJson);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testHandleTimestampSetType_validArray() {
        String jsonString = "{\"timestamps\": [\"2024-12-04T12:34:56.123Z\", \"2024-12-05T13:45:00.000Z\"]}";
        JSONObject valuesJson = new JSONObject(jsonString);

        Set<Timestamp> result = CassandraTypeHandler.handleTimestampSetType("timestamps", valuesJson);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains(Timestamp.valueOf("2024-12-04 12:34:56.123")));
        assertTrue(result.contains(Timestamp.valueOf("2024-12-05 13:45:00.0")));
    }
}
