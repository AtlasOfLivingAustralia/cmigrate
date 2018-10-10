package au.org.ala.cmigrate;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AccumWriterThread implements Runnable {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final AtomicInteger rowCount;
	private final CassandraCluster cassandraCluster;
    private final BlockingQueue<Map<String, String>> queue;
	private final Object sentinel;
    private final Session session;
    private final String keySpace;
    private final String columnFamily;
    private final Set<String> fieldSet;
    private final BlockingQueue<ResultRecordPair> resultQueue;
    private final BlockingQueue<ResultRecordPair> retryQueue;
    private Cache<String, PreparedStatement> preparedStatementCache;



    public AccumWriterThread(AtomicInteger rowCount, CassandraCluster cassandraCluster, BlockingQueue<Map<String, String>> queue, Object
            sentinel, Set<String> fieldSet, BlockingQueue<ResultRecordPair> resultQueue, BlockingQueue<ResultRecordPair> retryQueue) {
        this.rowCount = rowCount;
        this.cassandraCluster = cassandraCluster;
		this.queue = queue;
        this.sentinel = sentinel;
        this.fieldSet = fieldSet;
        this.session = cassandraCluster.getSession();
        this.keySpace  = cassandraCluster.getKeySpace();
        this.columnFamily = cassandraCluster.getColumnFamily();
        this.resultQueue = resultQueue;
        this.retryQueue = retryQueue;
        preparedStatementCache = Caffeine.newBuilder()
//                .softValues()
//                .weakKeys()
                .recordStats()
                .expireAfterAccess(15, TimeUnit.MINUTES)
                .maximumSize(2000)
                .build();
    }

	@Override
    public void run() {
        Boolean record_saved = true;
        int noRowsFoundCount = 1;
        LinkedHashMap<String, String> row = null;
        Object obj = null;
        PreparedStatement occUuidPrepared = cassandraCluster.getOccUuidPreparedStatement();
        while (true) {
            try {
                if (Thread.currentThread().isInterrupted()) {
                    log.error("Writer thread was interrupted, returning early");
                    break;
                }
                if (record_saved) {
                    if(retryQueue.isEmpty())
                        obj = queue.poll(5, TimeUnit.SECONDS);
                    else{
                        ResultRecordPair retryRecord = retryQueue.poll(1, TimeUnit.SECONDS);
                        if (retryRecord == null) {
                            log.info("No rows found in retry queue for this thread. Another thread may got the item.");
                            continue;
                        } else{
                            obj = retryRecord.getRecord();
                        }
                    }
                    if (obj == sentinel) {
                        log.info("Writer thread terminating cleanly on sentinel");
                        break;
                    }
                    if (obj == null) {
                        log.info("No rows found by writer thread for {} seconds", noRowsFoundCount++ * 5);
                        continue;
                    } else {
                        row = new LinkedHashMap<String, String>( (Map<String, String>)obj);
                    }
                    // Reset the count of no rows found in a sequence
                    noRowsFoundCount = 1;
                }

                if (occUuidPrepared != null){
                    insertIntoOccUuid(occUuidPrepared, row.get("uuid"), row.get("rowkey"));
                }

                row.remove("uuid");
                String query = "insert into " + keySpace + "." + columnFamily + " (\"" + StringUtils.join(row.keySet(), "\",\"")
                        + "\") values (" + StringUtils.join(Collections.nCopies(row.size(), "?"), ",") + ")";
                PreparedStatement prepared = preparedStatementCache.get(StringUtils.join(new TreeSet<String>(row.keySet()), "\",\""), k -> session.prepare(query));
                BoundStatement boundStatement = prepared.bind(row.values().toArray());
                ResultSetFuture result = session.executeAsync(boundStatement);
                resultQueue.put(new ResultRecordPair(row, result, columnFamily));

                if (log.isDebugEnabled()) {
                    log.debug("Record written key: {}", row.get("rowkey"));
                }

                if (rowCount.incrementAndGet() % 10000 == 0) {
                    log.info("Records Written: {}, QUEUE# {}, PS.CACHE.HIT.RATIO% {}, PS.CACHE.LOAD# {}, PS.CACHE.EVICTION# {}", rowCount.get(), this.queue.size(), this
                            .preparedStatementCache.stats().hitRate()*100, this.preparedStatementCache.stats().loadCount(), this.preparedStatementCache.stats().evictionCount());
                }
                record_saved = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
                e.printStackTrace();
                throw new RuntimeException(e);
            } catch (InvalidQueryException e) { // Column is not defined, trying to add and retry
                log.warn(e.getMessage());
                record_saved = false;
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
                record_saved = false;
            }
        }
        log.info("Records Written: {}, QUEUE# {}", rowCount.get(), this.queue.size());
        log.info("Thread exiting...");
    }

    private void insertIntoOccUuid(PreparedStatement prepared, String rowkey, String uuid) throws InterruptedException {
        Map<String, String> record = new LinkedHashMap<>();
        record.put("rowkey", rowkey);
        record.put("value", uuid);

        BoundStatement boundStatement = prepared.bind(record.values().toArray());
        ResultSetFuture result = session.executeAsync(boundStatement);
    }

    public static class WriterThreadBuilder{
        private final Logger log = LoggerFactory.getLogger(this.getClass());

        private  CassandraCluster cassandraCluster;
        private  BlockingQueue<Map<String, String>> queue;
        private  AtomicInteger rowCount;
        private  Object sentinel;
        private Set<String> fieldSet;
        private BlockingQueue<ResultRecordPair> resultQueue;
        private BlockingQueue<ResultRecordPair> retryQueue;

        public WriterThreadBuilder setQueue(BlockingQueue<Map<String, String>> queue) {
            this.queue = queue;
            return(this);
        }

        public WriterThreadBuilder setCassandraCluster(CassandraCluster cassandraCluster) {
            this.cassandraCluster = cassandraCluster;
            return(this);
        }

        public WriterThreadBuilder setRowCount(AtomicInteger rowCount) {

            this.rowCount = rowCount;
            return(this);
        }

        public WriterThreadBuilder setSentinel(Object sentinel) {
            this.sentinel = sentinel;
            return(this);
        }

        public AccumWriterThread createWriterThread(){
            return ( new AccumWriterThread(rowCount, cassandraCluster,  queue, sentinel, fieldSet, resultQueue, retryQueue));
        }


        public WriterThreadBuilder setFieldSet(Set<String> fieldSet) {

            this.fieldSet = fieldSet;
            return this;
        }

        public WriterThreadBuilder setResultQueue(BlockingQueue<ResultRecordPair> resultQueue) {
            this.resultQueue = resultQueue;
            return this;
        }
        public WriterThreadBuilder setRetryQueue(BlockingQueue<ResultRecordPair> retryQueue) {
            this.retryQueue = retryQueue;
            return this;
        }
    }
}
