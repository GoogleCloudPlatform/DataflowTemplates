package de.tillhub.coders;

import com.google.api.services.bigquery.model.TableRow;
import de.tillhub.mappers.FlatMapper;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TableRowsArrayJsonCoder extends AtomicCoder<TableRow[]> {

    private static final TableRowsArrayJsonCoder INSTANCE;

    static {
        INSTANCE = new TableRowsArrayJsonCoder();
    };

    public static TableRowsArrayJsonCoder of() {
        return INSTANCE;
    }

    @Override
    public void encode(TableRow[] value, OutputStream outStream) throws CoderException, IOException {
        throw new CoderException("not implemented");
    }



    @Override
    public TableRow[] decode(InputStream inStream) throws CoderException, IOException {
        return this.decode(inStream, Context.NESTED);
    }

    public TableRow[] decode(InputStream inStream, Context context) throws IOException {
        String strValue = StringUtf8Coder.of().decode(inStream, context);
        FlatMapper mapper = new FlatMapper(strValue);
        return mapper.readValues();
    }

}
