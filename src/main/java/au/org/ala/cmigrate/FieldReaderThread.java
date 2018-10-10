package au.org.ala.cmigrate;

import au.org.ala.cmigrate.maps.AbstractMapper;
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FieldReaderThread implements Runnable {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final String clusterName;
	private final String hostIp;
	private final String cfName;
	private final String keySpaceName;
	private final int pageSize;
	private final String startKey;
	private final String endKey;
	private final AbstractMapper cmapper;
	private final AtomicInteger rowCount;
	private final Set fieldSet;
	private Keyspace keyspace = null;
	private Cluster myCluster = null;
	private RangeSlicesQuery<String, String, String> allRowsQuery;
	private String lastKeyForMissing;

	public FieldReaderThread(String clusterName, String hostIp, int pageSize,
                             String startKey, String endKey, String cfName,
                             String keySpaceName, AbstractMapper cmapper, AtomicInteger rowCount, Set fieldSet) {
		this.clusterName = clusterName;
		this.hostIp = hostIp;
		this.pageSize = pageSize;
		this.startKey = startKey;
		this.endKey = endKey;
		this.cfName = cfName;
		this.keySpaceName = keySpaceName;
		this.cmapper = cmapper;
		this.rowCount = rowCount;
        this.fieldSet = fieldSet;
    }

	public void init() {
        myCluster = HFactory.getOrCreateCluster(clusterName, hostIp);
        keyspace = HFactory.createKeyspace(keySpaceName, myCluster, new AllOneConsistencyLevelPolicy());
        lastKeyForMissing = startKey;
        StringSerializer s = StringSerializer.get();
        allRowsQuery = HFactory.createRangeSlicesQuery(keyspace, s, s, s);
        allRowsQuery.setColumnFamily(cfName);
        allRowsQuery.setRange("", "", false, Integer.MAX_VALUE);
        allRowsQuery.setRowCount(pageSize);
    }

	private Map<String, String> mapColumns(Row<String, String, String> row) throws InterruptedException {

		Map<String, String> colvals = new TreeMap<String, String>();
        Map<String, String> record = new TreeMap<String, String>();

        ListIterator<HColumn<String, String>> columnIterator = row.getColumnSlice().getColumns().listIterator();
        while (columnIterator.hasNext()){
            HColumn<String, String> column = columnIterator.next();
            record.put(column.getName(), column.getValue());
        }

		for (Map.Entry<String, String> entry : record.entrySet()) {
			colvals.put(cmapper.get(entry.getKey()), entry.getValue());
		}

        if (!colvals.containsKey("rowkey") || colvals.get("rowkey") == null) {
            colvals.put("rowkey", row.getKey());
        }

        if (cfName.equals("occ") && (colvals.get("rowkey") == null || colvals.get("dataResourceUid") == null)) {
			return (null);
		}
		return (colvals);
	}

	@Override
	public void run() {
		init();
		try {
			while (true) {
				if (Thread.currentThread().isInterrupted()) {
					log.error("Reader thread was interrupted, returning early");
					break;
				}
				allRowsQuery.setKeys(lastKeyForMissing, endKey);
				QueryResult<OrderedRows<String, String, String>> res = allRowsQuery.execute();
				OrderedRows<String, String, String> rows = res.get();
				lastKeyForMissing = rows.peekLast().getKey();
				for (Row<String, String, String> aRow : rows) {
					if (Thread.currentThread().isInterrupted()) {
						log.error("Reader thread was interrupted, returning early");
						break;
					}
					log.debug("Record read key: {}", aRow.getKey());
					if (rowCount.incrementAndGet() % 10000 == 0) {
						log.info("Records Read: {}", rowCount);
					}
                    Map<String, String>  newRow = mapColumns(aRow);
					if (newRow != null) {
					    int fieldCount = fieldSet.size();
					    fieldSet.addAll(newRow.keySet());
					    if (fieldCount != fieldSet.size()){
					        log.info("New field(s) found. Count:{}", fieldSet.size());
                        }
					} else {
						log.debug("The row could not be mapped: {}", aRow.getKey());
					}
				}
				if (rows.getCount() != pageSize) {
					// end of the column family
					break;
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
	}


	public static class ReaderThreadBuilder {
		private final Logger log = LoggerFactory.getLogger(this.getClass());

		private  String clusterName;
		private  String hostIp;
		private  String cfName;
		private  String keySpaceName;
		private  int pageSize;
		private  String startKey;
		private  String endKey;
		private  Set fieldSet;

		private  AtomicInteger rowCount;

        private AbstractMapper mapper;


        public ReaderThreadBuilder(){

		}

        public ReaderThreadBuilder setClusterName(String clusterName) {
            this.clusterName = clusterName;
            return(this);
        }

        public ReaderThreadBuilder setFieldSet(Set fieldSet) {
            this.fieldSet= fieldSet;
            return(this);
        }

        public ReaderThreadBuilder setHostIp(String hostIp) {
			this.hostIp = hostIp;
			return(this);
		}

		public ReaderThreadBuilder setCfName(String cfName) {
			this.cfName = cfName;
			return(this);
		}

		public ReaderThreadBuilder setKeySpaceName(String keySpaceName) {
			this.keySpaceName = keySpaceName;
			return(this);
		}

		public ReaderThreadBuilder setPageSize(int pageSize) {
			this.pageSize = pageSize;
			return(this);
		}

		public ReaderThreadBuilder setStartKey(String startKey) {
			this.startKey = startKey;
			return(this);
		}

		public ReaderThreadBuilder setEndKey(String endKey) {
			this.endKey = endKey;
			return(this);
		}

		public ReaderThreadBuilder setRowCount(AtomicInteger rowCount) {
			this.rowCount = rowCount;
			return(this);
		}

        public ReaderThreadBuilder setMapper(AbstractMapper mapper) {
            this.mapper = mapper;
            return(this);
        }

        public FieldReaderThread createReaderThread(){
		    return(new FieldReaderThread(clusterName, hostIp, pageSize, startKey, endKey, cfName, keySpaceName, mapper, rowCount, fieldSet));
        }
	}

}
