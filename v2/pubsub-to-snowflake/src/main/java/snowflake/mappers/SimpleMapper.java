package snowflake.mappers;

import org.apache.beam.sdk.io.snowflake.SnowflakeIO;

import java.util.Arrays;

public class SimpleMapper {
    // Mapper for mapping PCollection data to array of string
    public  SnowflakeIO.UserDataMapper<String> mapper(){
        return (SnowflakeIO.UserDataMapper<String>) recordLine -> Arrays.stream(recordLine.split(",", -1))
                .map(column -> {
                    if(column.equals(""))
                        return null;
                    else if (column.contains("UTC"))
                        return column.replace(" UTC","");
                    return column;
                } )
                .toArray();
    }

    // Mapper for mapping PCollection data to array of string
    public SnowflakeIO.CsvMapper<String> getCsvMapper(){
        return (SnowflakeIO.CsvMapper<String>) record -> String.join(",", record);
    }

}
