package au.org.ala.cmigrate;

import java.util.Map;


public  class ErrorRecord{
    final Map<String, String> record;
    final String columnFamily;

    public ErrorRecord(Map<String, String> record, String columnFamily) {
        this.record = record;
        this.columnFamily = columnFamily;
    }

    public Map<String, String> getRecord() {
        return record;
    }

    public String getColumnFamily() {
        return columnFamily;
    }
}