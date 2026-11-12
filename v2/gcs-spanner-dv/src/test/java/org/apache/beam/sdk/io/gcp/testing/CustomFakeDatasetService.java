package org.apache.beam.sdk.io.gcp.testing;

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableRow;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class CustomFakeDatasetService extends FakeDatasetService {
    public CustomFakeDatasetService() {}

    @Override
    public java.util.Map<TableRow, List<InsertErrors>> getInsertErrors() {
        return new HashMap<TableRow, List<InsertErrors>>() {
            @Override
            public List<InsertErrors> get(Object key) {
                if (key instanceof TableRow) {
                    TableRow actual = (TableRow) key;
                    if (actual.containsKey("run_id") && actual.containsKey("status")) {
                        return Collections.singletonList(
                            new InsertErrors().setErrors(
                                Collections.singletonList(
                                    new ErrorProto().setReason("invalid")
                                )
                            )
                        );
                    }
                }
                return super.get(key);
            }
        };
    }
}
