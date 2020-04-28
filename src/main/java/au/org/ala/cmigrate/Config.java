package au.org.ala.cmigrate;

import java.util.Map;

public class Config {
    private String startKey;
    private String endKey;
    private int pageSize;
    private int readThreads;
    private int writeThreads;
    private boolean syncSchema;
    private String solrBase;
    private Map<String, String> source;
    private Map<String, String> target;

    public String getStartKey() {
        return startKey;
    }

    public void setStartKey(String startKey) {
        this.startKey = startKey;
    }

    public String getEndKey() {
        return endKey;
    }

    public void setEndKey(String endKey) {
        this.endKey = endKey;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getReadThreads() {
        return readThreads;
    }

    public void setReadThreads(int readThreads) {
        this.readThreads = readThreads;
    }

    public int getWriteThreads() {
        return writeThreads;
    }

    public void setWriteThreads(int writeThreads) {
        this.writeThreads = writeThreads;
    }

    public boolean isSyncSchema() {
        return syncSchema;
    }

    public void setSyncSchema(boolean syncSchema) {
        this.syncSchema = syncSchema;
    }

    public String getSolrBase() {
        return solrBase;
    }

    public void setSolrBase(String solrBase) {
        this.solrBase = solrBase;
    }

    public Map<String, String> getSource() {
        return source;
    }

    public void setSource(Map<String, String> source) {
        this.source = source;
    }

    public Map<String, String> getTarget() {
        return target;
    }

    public void setTarget(Map<String, String> target) {
        this.target = target;
    }
}
