package au.org.ala.cmigrate;

import com.datastax.driver.core.ResultSetFuture;

import java.util.Map;

/**
 * Created by Mahmoud Sadeghi on 23/8/17.
 */
public class ResultRecordPair {
        private final Map<String, String> record;
        private final ResultSetFuture result;
        private final String columnFamily;
        private int retryCount = 0;

    public Map<String, String> getRecord() {
        return record;
    }


    public int getRetryCount() {
        return retryCount;
    }

    public int incrementAndGetRetryCount() {
        retryCount++;
        return retryCount;
    }

    public ResultSetFuture getResult() {
        return result;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public ResultRecordPair(Map<String, String> record, ResultSetFuture result, String columnFamily) {
        this.record = record;
        this.result = result;
        this.columnFamily = columnFamily;
    }
}
