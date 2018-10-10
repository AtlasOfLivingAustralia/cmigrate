package au.org.ala.cmigrate;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Writes rows containing errors out using JSON serialisation for each row, to
 * an aggregated error file.
 * <p>
 * Created by Mahmoud Sadeghi on 8/5/17.
 */
public class ErrorLoggerThread implements Runnable {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final BlockingQueue<ErrorRecord> errorQueue;
    private final CassandraCluster cassandraCluster;
    private final Object sentinel;
    private Session session;
    private String keySpace;
    private String columnFamily;
    private Cache<String, PreparedStatement> cache;
    private final AtomicInteger recordCount;

    public ErrorLoggerThread(BlockingQueue<ErrorRecord> errorQueue, CassandraCluster cassandraCluster, Object sentinel, AtomicInteger recordCount) {
        this.errorQueue = errorQueue;
        this.sentinel = sentinel;
        this.cassandraCluster = cassandraCluster;
        this.recordCount = recordCount;
    }

    public void init() {
        cache = Caffeine.newBuilder()
                .maximumSize(200)
                .build();
        session = cassandraCluster.getSession();
        this.keySpace = cassandraCluster.getKeySpace();
        this.columnFamily = cassandraCluster.getColumnFamily() + "_error_log";
   }

    @Override
    public void run() {
        ErrorRecord errorObject = null;
        Map<String, String> recMap;
        Boolean record_saved = true;
        Map<String, String> record = null;
        log.info("Thread started.");
        while (true) {
            try {
                if (record_saved) {
                    if (recordCount.incrementAndGet() % 10000 == 0) {
                        log.info("Errors Written: {}, QUEUE# {}", recordCount.get(), errorQueue.size());
                    }
                    Object obj = errorQueue.poll(1, TimeUnit.MINUTES);
                    if (obj == null) {
                        log.info("Got null object, retrying. QUEUE# {}", errorQueue.size());
                        continue;
                    } else if (obj == sentinel) {
                        log.info("{} exiting on sentinel. QUEUE# {}", errorQueue.size());
                        break;
                    }
                    errorObject = (ErrorRecord) obj;
                    recMap = errorObject.getRecord();

                    record = new TreeMap<>();
                    if(recMap != null) {
                        for (Map.Entry<String, String> entry : recMap.entrySet()) {
                            record.put(entry.getKey().replaceAll("\\.", "_"), entry.getValue());
                        }
                        record_saved = true;
                    }

                }

                if (record.size() == 0)
                    continue;
                String query = "insert into " + keySpace + "." + errorObject.getColumnFamily() + " (rowuid, \"" + StringUtils.join(record.keySet(), "\",\"")
                        + "\") values (blobAsUuid(timeuuidAsBlob(now())), " + StringUtils.join(Collections.nCopies(record.size(), "?"), ",") + ")";
                PreparedStatement prepared = cache.get(query, k -> session.prepare(query));
                BoundStatement boundStatement = prepared.bind(record.values().toArray());
                this.cassandraCluster.getSession().executeAsync(boundStatement);
                log.debug("A row is logged into " + errorObject.getColumnFamily() + ".");
                if (log.isDebugEnabled()) {
                    log.warn("A problematic record has been written in the ColumnFamily: {}", errorObject.getColumnFamily());
                }
                record_saved = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (InvalidQueryException e) { // Column is not defined, trying to add and retry
                log.warn(e.getMessage());
                record_saved = false;
                try{
                    if (e.getMessage().startsWith("Undefined column name ")) {
                        String columnName = e.getMessage().replace("Undefined column name ", "");
                        session.execute("ALTER TABLE " + keySpace + "." + errorObject.getColumnFamily() + " ADD " + columnName + " text");
                    } else if (e.getMessage().startsWith("unconfigured table ")) {
                        session.execute("CREATE TABLE IF NOT EXISTS " + keySpace + "." + e.getMessage().replace("unconfigured table ", "")  + " (rowuid uuid primary key)");
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    } else {
                        e.printStackTrace();
                    }
                }catch (Exception e1){
                    log.warn("An exception occurred:", e1);
                }

            }
        }
        log.info("Thread ended.");
    }
}

