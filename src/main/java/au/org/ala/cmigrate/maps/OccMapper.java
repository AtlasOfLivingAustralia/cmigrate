package au.org.ala.cmigrate.maps;

import java.util.*;

public class OccMapper extends AbstractMapper{

    @Override
    public String getCreateCQL(){
        return ("CREATE TABLE IF NOT EXISTS " + keyspace + "." + tableName + " ( rowkey varchar, \"dataResourceUid\" varchar, PRIMARY KEY (rowkey, \"dataResourceUid\")) ");
    }

    public OccMapper(String keySpace, String tableName) {
        super(keySpace, tableName);
        this.put("generalisedMeters", "generalisationToApplyInMetres");
        this.put("rowKey", "rowkey");
    }

    @Override
    public List<String> getDefaultFieldList() {
        Set<String> ts = new LinkedHashSet<>();
        ts.add("rowkey");
        ts.add("dataResourceUid");
        return new ArrayList<>(ts);
    }


}
