package au.org.ala.cmigrate;

import com.datastax.driver.core.*;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;

public class CassandraCluster {

    private Cluster cluster;
    private Session session;
    private final String keySpace;
    private final String columnFamily;
    private final String clusterName;
    private final String[] hosts;
    private PreparedStatement preparedStatement;
    private PreparedStatement occUuidPreparedStatement = null;
    private final TreeMap<String, String> cmapper;


    public CassandraCluster(String keySpace, String columnFamily, String clusterName, String[] hosts, TreeMap<String, String> cmapper) {
        this.keySpace = keySpace;
        this.columnFamily = columnFamily;
        this.clusterName = clusterName;
        this.hosts = hosts;
        this.cmapper = cmapper;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Session getSession() {
        return session;
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }
    public PreparedStatement getOccUuidPreparedStatement() {
        return occUuidPreparedStatement;
    }
    public String getKeySpace() {
        return keySpace;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void init() {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setHeartbeatIntervalSeconds(60)
                      .setMaxQueueSize(1024)
                       .setConnectionsPerHost(HostDistance.LOCAL,  4, 10);

        cluster = Cluster.builder().addContactPoints(hosts).withClusterName(clusterName)
                .withMaxSchemaAgreementWaitSeconds(60)
                .withPoolingOptions(poolingOptions)
                .build();
        session = cluster.connect();
    }
    public void prepareInsertStatement(Set<String> fieldSet){
        String query = "insert into " + keySpace + "." + columnFamily + " (\"" + StringUtils.join(fieldSet, "\",\"")
                + "\") values (" + StringUtils.join(Collections.nCopies(fieldSet.size(), "?"), ",") + ")";
        preparedStatement = session.prepare(query);
        if(columnFamily.equals("occ")) {
            occUuidPreparedStatement = session.prepare("insert into " + keySpace + ".occ_uuid (rowkey, value) values (?,?)");
        }
    }

    public static class CassandraClusterBuilder {
        private String keySpace;
        private String columnFamily;
        private  String clusterName;
        private  String[] hosts;

        private  TreeMap<String, String> cmapper;

        public CassandraClusterBuilder setKeySpace(String keySpace) {
            this.keySpace = keySpace;
            return(this);
        }

        public CassandraClusterBuilder setColumnFamily(String columnFamily) {
            this.columnFamily = columnFamily;
            return(this);
        }

        public CassandraClusterBuilder setClusterName(String clusterName) {
            this.clusterName = clusterName;
            return(this);
        }

        public CassandraClusterBuilder setHosts(String[] hosts) {
            this.hosts = hosts;
            return(this);
        }

        public CassandraClusterBuilder setCmapper(TreeMap<String, String> cmapper) {
            this.cmapper = cmapper;
            return(this);
        }

        public CassandraCluster createCassandraCluster(){
            return(new CassandraCluster(this.keySpace, this.columnFamily, this.clusterName, this.hosts, this.cmapper));
        }

    }
}
