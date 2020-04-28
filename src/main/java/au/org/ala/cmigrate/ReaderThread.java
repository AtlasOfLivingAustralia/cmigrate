package au.org.ala.cmigrate;

import au.org.ala.cmigrate.maps.AbstractMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReaderThread implements Runnable {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final String clusterName;
	private final String hostIp;
	private final String cfName;
	private final String keySpaceName;
	private final int pageSize;
	private final BlockingQueue<Map<String, String>> queue;
	private final BlockingQueue errorQueue;
	private final String startKey;
	private final String endKey;
	private final AbstractMapper cmapper;
	private final AtomicInteger rowCount;
	private Keyspace keyspace = null;
	private Cluster myCluster = null;
	private RangeSlicesQuery<String, String, String> allRowsQuery;
	private String lastKeyForMissing;

	public ReaderThread(String clusterName, String hostIp, int pageSize, BlockingQueue<Map<String, String>> queue,
						BlockingQueue<ErrorRecord> errorQueue, String startKey, String endKey, String cfName,
						String keySpaceName, AbstractMapper cmapper, AtomicInteger rowCount) {
		this.clusterName = clusterName;
		this.hostIp = hostIp;
		this.pageSize = pageSize;
		this.queue = queue;
		this.startKey = startKey;
		this.endKey = endKey;
		this.errorQueue = errorQueue;
		this.cfName = cfName;
		this.keySpaceName = keySpaceName;
		this.cmapper = cmapper;
		this.rowCount = rowCount;
	}

	public void init() {
		CassandraHostConfigurator conf = new CassandraHostConfigurator(this.hostIp);
		conf.setCassandraThriftSocketTimeout(120000); // 1 minute
		conf.setRetryDownedHostsDelayInSeconds(10);
		conf.setRetryDownedHostsQueueSize(128);
		conf.setRetryDownedHosts(true);

        myCluster = HFactory.getOrCreateCluster(this.clusterName, conf);
        keyspace = HFactory.createKeyspace(this.keySpaceName, myCluster, new AllOneConsistencyLevelPolicy());
        lastKeyForMissing = this.startKey;
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
            if (!column.getName().endsWith(".p")) {
                record.put(column.getName(), column.getValue());
            }
            if (!column.getName().equalsIgnoreCase("sensitive")) {
                record.put(column.getName(), column.getValue());
            }
        }

		for (Map.Entry<String, String> entry : record.entrySet()) {
			colvals.put(cmapper.get(entry.getKey()), entry.getValue());
		}

		if (!colvals.containsKey("rowkey") || colvals.get("rowkey") == null) {
			colvals.put("rowkey", row.getKey());
		}
		if (colvals.containsKey("originalSensitiveValues") || colvals.get("originalSensitiveValues") != null) {
            String originalSensitiveValues = colvals.get("originalSensitiveValues");
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode fields = mapper.readTree(originalSensitiveValues);
                if (fields != null){
                    for (Iterator<Map.Entry<String, JsonNode>> it = fields.fields(); it.hasNext(); ) {
                        Map.Entry<String, JsonNode> field = it.next();
                        if (!field.getKey().endsWith(".p")) {
                            colvals.put(field.getKey(), field.getValue().textValue());
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                colvals.remove("originalSensitiveValues");
            }
		}


		if (cfName.equals("occ")) {
        	String rowkey = colvals.get("rowkey");
			if (rowkey == null) {
				ErrorRecord errorRecord = new ErrorRecord(record, cfName + "_error_log");
				if (!this.errorQueue.offer(errorRecord, 5, TimeUnit.SECONDS)) {
					log.error("ERROR QUEUE IS FULL: waiting for capacity to add error key: {}", row.getKey());
					this.errorQueue.put(errorRecord);
				}
				log.debug("Mapping error for a record, will be logged in the log file.");
				return (null);
			} else {
				if (colvals.get("dataResourceUid") == null) { // extract dataResourceUid from rowkey
					Pattern pattern = Pattern.compile("(dr\\d+)\\|");
					Matcher matcher = pattern.matcher(rowkey);
					if (matcher.find())
						colvals.put("dataResourceUid", matcher.group(1));
				}
			}
			if (colvals.get("uuid") == null) {
				ErrorRecord errorRecord = new ErrorRecord(record, cfName + "_error_log");
				if (!this.errorQueue.offer(errorRecord, 5, TimeUnit.SECONDS)) {
					log.error("ERROR QUEUE IS FULL: waiting for capacity to add error key: {}", row.getKey());
					this.errorQueue.put(errorRecord);
				}
				log.debug("Mapping error for a record, will be logged in the log file.");
				return (null);
			}else{
				colvals.put("rowkey", colvals.get("uuid"));
				colvals.put("uuid",rowkey);
			}
		}
		if (cfName.equals("qa") && colvals.get("userId") == null) {
			ErrorRecord errorRecord = new ErrorRecord(record, cfName + "_error_log");
			if (!this.errorQueue.offer(errorRecord, 5, TimeUnit.SECONDS)) {
				log.error("ERROR QUEUE IS FULL: waiting for capacity to add error key: {}", row.getKey());
				this.errorQueue.put(errorRecord);
			}
			log.debug("Mapping error for a record, will be logged in the log file.");
			return (null);
		}

		return (colvals);
	}

	@Override
	public void run() {
		init();
		try {
			while (true) {
				try {
					if (Thread.currentThread().isInterrupted()) {
						log.error("Reader thread was interrupted, returning early");
						break;
					}
					allRowsQuery.setKeys(lastKeyForMissing, this.endKey);
					QueryResult<OrderedRows<String, String, String>> res = allRowsQuery.execute();
					OrderedRows<String, String, String> rows = res.get();
					lastKeyForMissing = rows.peekLast().getKey() + "\t";
					for (Row<String, String, String> aRow : rows) {
						if (Thread.currentThread().isInterrupted()) {
							log.error("Reader thread was interrupted, returning early");
							break;
						}
						Map<String, String> newRow = null;
						log.debug("Record read key: {}", aRow.getKey());
						if (rowCount.incrementAndGet() % 10000 == 0) {
							log.info("Records Read: {}, QUEUE# {}", rowCount, this.queue.size());
						}
						newRow = mapColumns(aRow);
						if (newRow != null) {
							if (!this.queue.offer(newRow, 5, TimeUnit.SECONDS)) {
								if (Thread.currentThread().isInterrupted()) {
									log.error("Reader thread was interrupted, returning early");
									break;
								}
								log.info("QUEUE IS FULL: waiting for capacity to add key: {}", aRow.getKey());
								this.queue.put(newRow);
							}
						} else {
							log.debug("The row could not be mapped: {}", aRow.getKey());
						}
					}
					if (rows.getCount() != pageSize) {
						// end of the column family
						break;
					}
				}catch (HectorException e){
					log.warn("An error occurred while reading from the source db",e);
					try {
						Thread.sleep(5000);
					}catch (InterruptedException ex){
						log.warn("An error occurred while thread sleeping",ex);

					}

				}
			}
			log.info("Records Read: {}, QUEUE# {}", rowCount, this.queue.size());
			log.info("ReaderThread exiting....");

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
		private  BlockingQueue<Map<String, String>> queue;
        private BlockingQueue<ErrorRecord> errorQueue;
		private  String startKey;
		private  String endKey;

		private  AtomicInteger rowCount;

        private AbstractMapper cmapper;


		public ReaderThreadBuilder(){

		}

		public ReaderThreadBuilder setClusterName(String clusterName) {
			this.clusterName = clusterName;
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

		public ReaderThreadBuilder setQueue(BlockingQueue<Map<String, String>> queue) {
			this.queue = queue;
			return(this);
		}

		public ReaderThreadBuilder setErrorQueue(BlockingQueue<ErrorRecord> errorQueue) {
			this.errorQueue = errorQueue;
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

		public ReaderThreadBuilder setCmapper(AbstractMapper cmapper) {
			this.cmapper = cmapper;
			return(this);
		}


		public ReaderThread createReaderThread(){
		    return(new ReaderThread(this.clusterName, this.hostIp, this.pageSize, this.queue, this.errorQueue, this.startKey, this.endKey, this.cfName, this.keySpaceName, this
					.cmapper, this.rowCount));
        }
	}

}
