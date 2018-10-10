package au.org.ala.cmigrate.maps;

import java.util.*;

/**
 * Created by Mahmoud Sadeghi on 14/8/17.
 */
public abstract class AbstractMapper  extends TreeMap<String, String> {
    protected String tableName = "";
    protected String keyspace = "";

    public AbstractMapper(String keyspace, String tableName) {
        super();
        this.tableName = tableName;
        this.keyspace= keyspace;
        this.put("rowKey", "rowkey");
    }
    @Override
    public String get(Object key) {
        String value = super.get(key);

        return (value != null)? value : ((String)key).replaceAll("\\.", "_");
    }

    public List<String> getDefaultFieldList() {
        Set<String> ts = new LinkedHashSet<>();
        ts.add("rowkey");
        return new ArrayList<>(ts);
    }

    public String getCreateCQL(){
        return ("CREATE TABLE IF NOT EXISTS " + keyspace + "." + tableName + " ( rowkey varchar PRIMARY KEY)");
    }
}
