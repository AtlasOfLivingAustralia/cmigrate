package au.org.ala.cmigrate;

import au.org.ala.cmigrate.maps.*;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CMigrate {

    private static final Logger log = LoggerFactory.getLogger(CMigrate.class);

    private static final int DEFAULT_QUEUE_SIZE = 1000;
    private static final String DEFAULT_APP_CONFIG = "./cass1-cass3.yaml";

    public static Map<String, String> getOccKeyRanges(final String startKey, final String endKey, final int readThreads,
                                                      final String solrBase) throws IOException, SolrServerException {

        Map<String, String> map = new TreeMap<>();
        if (readThreads == 1) {
            map.put(startKey, endKey);
            return map;
        }
        String cassKey = startKey;

        String startq = (startKey.equals("")) ? "*" : startKey;
        String endq = (endKey.equals("~")) ? "*" : endKey;

        String q = null;
        String solrStartRowkey = startq.equals("*") ? startq : ("\"" + startq + "\"");
        String solrEndRowkey = endq.equals("*") ? endq : ("\"" + endq + "\"");
        q = "row_key:[" + solrStartRowkey + " TO " + solrEndRowkey + "]";
        // q = "q=" + URLEncoder.encode("row_key:['" + startq + "' TO '" + endq
        // + "']", "UTF-8");
        SolrClient solr = new HttpSolrClient.Builder(solrBase).build();
        SolrQuery query = new SolrQuery();
        query.setQuery(q);
        query.setRows(1);
        query.setFields("row_key");
        query.setSort("row_key", SolrQuery.ORDER.asc);
        try {
            QueryResponse queryResponse = solr.query(query);
            long totalRecords = queryResponse.getResults().getNumFound();
            log.info("Total number of records in SOLR query: {}", totalRecords);
            int i;
            for (i = 1; i < readThreads; i++) {
                query.setStart((int) (totalRecords / readThreads));
                query.setFilterQueries("row_key:[" + solrStartRowkey + " TO *]");
                queryResponse = solr.query(query);
                String key = (String) queryResponse.getResults().get(0).get("row_key");

                log.info("Range {} found [{} TO {}]", i, cassKey, key);

                map.put(cassKey, key);
                cassKey = key;
                solrStartRowkey = key.equals("*") ? key : ("\"" + key + "\"");
            }
            map.put(cassKey, endKey);
            log.info("Range {} found [{} TO {}]", i, cassKey, endKey);

            log.info("Ranges are set for each thread.");

        } catch (SolrServerException e) {
            e.printStackTrace();
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }

        return map;

    }

    private static Set<String> buildFieldList(String clusterName, String columnFamily, String keySpace, Map<String, String> keyRanges,
                                              AbstractMapper map, String sourceDb, int readThreads,
                                              int pageSize, String solrBase) throws InterruptedException, IOException, SolrServerException {
        log.info("Building the field list by iterating over the source db in the provided range.");
        readThreads = (readThreads <= 1) ? readThreads : readThreads * 2;
        if (columnFamily == "occ")
            keyRanges = getOccKeyRanges((String) keyRanges.keySet().toArray()[0], (String) (keyRanges.values().toArray()[keyRanges.size() - 1]), readThreads, solrBase);
        final ExecutorService readerExecutor = Executors.newFixedThreadPool(readThreads);
        Set<String> fieldSet = new HashSet<>(map.getDefaultFieldList());
        final FieldReaderThread.ReaderThreadBuilder readerBuilder = new FieldReaderThread.ReaderThreadBuilder()
                .setCfName(columnFamily)
                .setClusterName(clusterName)
                .setMapper(map)
                .setFieldSet(fieldSet)
                .setPageSize(pageSize)
                .setKeySpaceName(keySpace)
                .setHostIp(sourceDb)
                .setRowCount(new AtomicInteger(0));
        for (Map.Entry<String, String> entry : keyRanges.entrySet()) {
            readerExecutor.execute(readerBuilder.setStartKey(entry.getKey()).setEndKey(entry.getValue()).createReaderThread());
        }
        readerExecutor.shutdown();

        int readerExecutorWait = 0;
        while (!readerExecutor.awaitTermination(1, TimeUnit.MINUTES) && !Thread.currentThread().isInterrupted()
                && readerExecutorWait < 300) { // 5 hours
            readerExecutorWait++;
            log.warn("Reader threads not complete after {} minutes, waiting for them again", readerExecutorWait);
        }
        return fieldSet;

    }

    private static void addFieldsToColumnFamily(Set<String> fieldSet, AbstractMapper map, CassandraCluster cassandraCluster) {
        fieldSet.forEach(column -> {
            boolean success = false;
            while (!success) {
                try {
                    log.info("Adding the missing column \"" + column + "\"into the table \"" + cassandraCluster.getKeySpace() + "." + cassandraCluster.getColumnFamily() + "\"");
                    ResultSet result = cassandraCluster.getSession().execute("ALTER TABLE " + cassandraCluster.getKeySpace() + "." + cassandraCluster.getColumnFamily() + " ADD \"" +
                            column +
                            "\" text");
                    success = true;
                } catch (InvalidQueryException e1) {
                    log.warn(e1.getMessage());
                    success = true;
                } catch (OperationTimedOutException e) {
                    log.warn("Adding the missing column \"{}\"into the table \"{}.{}\" failed (retry again).  MESSAGE:{}", column, cassandraCluster.getKeySpace(),
                            cassandraCluster.getColumnFamily(), e
                                    .getMessage
                                            ());
                    success = false;
                }
            }
        });
    }

    private static void copyColumnFmaily(String clusterName, String columnFamily, String keySpace, Map<String, String> keyRanges,
                                         AbstractMapper map, String sourceDb, String[] targetDb, int readThreads,
                                         int writeThreads, int pageSize, String solrBase, boolean sync) throws InterruptedException, IOException, SolrServerException {

        final BlockingQueue<Map<String, String>> queue = new ArrayBlockingQueue<>(DEFAULT_QUEUE_SIZE);
        final BlockingQueue<ErrorRecord> errorQueue = new ArrayBlockingQueue<>(DEFAULT_QUEUE_SIZE);
        final BlockingQueue<ResultRecordPair> resultQueue = new ArrayBlockingQueue<>(DEFAULT_QUEUE_SIZE * 2);
        final BlockingQueue<ResultRecordPair> retryQueue = new ArrayBlockingQueue(DEFAULT_QUEUE_SIZE * 2);

        AtomicInteger writeCounter = new AtomicInteger(0);

        Object sentinel = new Object();

        final int resultCheckerThreads = (writeThreads / 2 > 0) ? writeThreads / 2 : 1;

        final CassandraCluster cassandraCluster = setupCassandraCluster(clusterName, columnFamily, keySpace, map, targetDb);

        final Set<String> fieldSet = prepareColumnFamilySchema(clusterName, columnFamily, keySpace, keyRanges, map, sourceDb, readThreads, pageSize, solrBase, sync,
                cassandraCluster);

        final ExecutorService resultExecutor = setupAndStartResultCheckers(queue, resultCheckerThreads, sentinel, resultQueue, errorQueue, writeCounter, retryQueue);

        final ExecutorService loggerExecutor = setupErrorLoggers(errorQueue, sentinel, cassandraCluster);

        final ExecutorService readerExecutor = setupAndStartReaders(clusterName, columnFamily, keySpace, keyRanges, map, queue, errorQueue, sourceDb, pageSize, readThreads);
        final ExecutorService writerExecutor = setupAndStartWriters(queue, writeThreads, sentinel, cassandraCluster, fieldSet, resultQueue, writeCounter, retryQueue);

        // Shutdown both executors so they know no more tasks are incoming,
        // but allow them to complete their current tasks

        readerExecutor.shutdown();
        writerExecutor.shutdown();
        resultExecutor.shutdown();
        loggerExecutor.shutdown();

        waitForExecutorCompletion("readerExecutor", readerExecutor);
        waitForSendingSentinel(writeThreads, queue, sentinel);
        waitForSendingSentinel(1, errorQueue, sentinel);

        waitForExecutorCompletion("writerExecutor", writerExecutor);
        waitForSendingSentinel(resultCheckerThreads, resultQueue, sentinel);

        waitForExecutorCompletion("resultExecutor", resultExecutor);
        waitForExecutorCompletion("loggerExecutor", loggerExecutor);

        log.info("All executors are shutdown.");
        try {
            cassandraCluster.getSession().close();
        } finally {
            cassandraCluster.getCluster().close();
        }
    }

    private static void waitForExecutorCompletion(String executorName, ExecutorService executor) {
        int executorWait = 0;
        try {
            while (!executor.awaitTermination(30, TimeUnit.MINUTES) && !Thread.currentThread().isInterrupted()
                    && executorWait < 144) {
                executorWait++;
                log.warn("Executor {} not complete after {} hours, waiting for them again", executorName, executorWait / 2.0);
            }
            if (executorWait < 144)
                log.info("Executor {} was completed successfully.", executorName);
        } catch (InterruptedException e) {
            log.error("InterruptedException occurred for {} ", executorName);
        } finally {
            if (!executor.isTerminated()) {
                log.error("Executor {} was not terminated cleanly, calling shutdownNow().", executorName);
                executor.shutdownNow();
            }
        }


    }

    private static void waitForSendingSentinel(int executorCount, BlockingQueue queue, Object sentinel) {
        int sentinelWait = 0;
        for (int i = 0; i < executorCount; i++) {
            try {
                while (!queue.offer(sentinel, 1, TimeUnit.MINUTES) && !Thread.currentThread().isInterrupted()
                        && sentinelWait < 60) {
                    sentinelWait++;
                    log.warn("Waiting for queue to accept sentinel ({} minutes elapsed)", sentinelWait);
                }
            } catch (InterruptedException e) {
                log.error("InterruptedException occurred");
            }
        }
    }

    private static ExecutorService setupErrorLoggers(BlockingQueue<ErrorRecord> errorQueue, Object sentinel, CassandraCluster cassandraCluster) {
        AtomicInteger recordCount = new AtomicInteger(0);
        final ErrorLoggerThread loggerRunnable = new ErrorLoggerThread(errorQueue, cassandraCluster, sentinel, recordCount);
        loggerRunnable.init();
        final Thread loggerThread = new Thread(loggerRunnable);


        final ExecutorService errorExecutor = Executors.newFixedThreadPool(1);
        errorExecutor.execute(loggerThread);
        return errorExecutor;

    }

    private static Set<String> prepareColumnFamilySchema(String clusterName, String columnFamily, String keySpace, Map<String, String> keyRanges, AbstractMapper map, String sourceDb, int readThreads, int pageSize, String solrBase, boolean sync, CassandraCluster cassandraCluster) throws InterruptedException, IOException, SolrServerException {
        Set<String> fieldSet = null;
        if (sync) {
            fieldSet = buildFieldList(clusterName, columnFamily, keySpace, keyRanges, map, sourceDb, readThreads, pageSize, solrBase);

            String fieldSetStr = StringUtils.join(fieldSet);
            Files.write(Paths.get("/tmp/fields.txt"), fieldSetStr.getBytes());

            addFieldsToColumnFamily(fieldSet, map, cassandraCluster);
        } else {
            fieldSet = getTargetFieldSet(cassandraCluster);
        }
        cassandraCluster.prepareInsertStatement(fieldSet);
        return fieldSet;
    }

    private static CassandraCluster setupCassandraCluster(String clusterName, String columnFamily, String keySpace, AbstractMapper map, String[] targetDb) {
        final CassandraCluster cassandraCluster = new CassandraCluster.CassandraClusterBuilder()
                .setClusterName(clusterName)
                .setCmapper(map)
                .setColumnFamily(columnFamily)
                .setHosts(targetDb)
                .setKeySpace(keySpace)
                .createCassandraCluster();
        cassandraCluster.init();
        //create the columnFamily if doesn't exist
        cassandraCluster.getSession().execute(map.getCreateCQL());
        return cassandraCluster;
    }

    private static ExecutorService setupAndStartResultCheckers(BlockingQueue queue, int resultCheckerThreads, Object sentinel, BlockingQueue<ResultRecordPair>
            resultQueue, BlockingQueue<ErrorRecord> errorQueue, AtomicInteger writeCount, BlockingQueue<ResultRecordPair> retryQueue) {
        final AtomicInteger verifiedRowCount = new AtomicInteger(0);
        final ExecutorService resultExecutor = Executors.newFixedThreadPool(resultCheckerThreads);
        for (int i = 0; i < resultCheckerThreads; i++) {
            resultExecutor.execute(new Thread(new ResultCheckerThread(resultQueue, queue, errorQueue, retryQueue, sentinel, writeCount, verifiedRowCount)));
        }
        return resultExecutor;
    }

    private static ExecutorService setupAndStartWriters(BlockingQueue queue, int writeThreads, Object sentinel, CassandraCluster cassandraCluster, Set<String> fieldSet,
                                                        BlockingQueue<ResultRecordPair> resultQueue, AtomicInteger writeCount, BlockingQueue<ResultRecordPair> retryQueue) {
        final ExecutorService writerExecutor = Executors.newFixedThreadPool(writeThreads);
        final AccumWriterThread.WriterThreadBuilder writerBuilder = new AccumWriterThread.WriterThreadBuilder()
                .setCassandraCluster(cassandraCluster)
                .setSentinel(sentinel)
                .setFieldSet(fieldSet)
                .setResultQueue(resultQueue)
                .setQueue(queue)
                .setRowCount(writeCount)
                .setRetryQueue(retryQueue);

        for (int i = 0; i < writeThreads; i++) {
            writerExecutor.execute(writerBuilder.createWriterThread());
        }

        return writerExecutor;
    }

    private static ExecutorService setupAndStartReaders(String clusterName, String columnFamily, String keySpace, Map<String, String> keyRanges, AbstractMapper map,
                                                        BlockingQueue queue, BlockingQueue errorqueue, String sourceDb, int pageSize, int readThreads) {
        final ExecutorService readerExecutor = Executors.newFixedThreadPool(readThreads);
        final ReaderThread.ReaderThreadBuilder readerBuilder = new ReaderThread.ReaderThreadBuilder()
                .setCfName(columnFamily)
                .setClusterName(clusterName)
                .setCmapper(map)
                .setErrorQueue(errorqueue)
                .setQueue(queue)
                .setPageSize(pageSize)
                .setKeySpaceName("occ")
                .setHostIp(sourceDb)
                .setRowCount(new AtomicInteger(0));
        for (Map.Entry<String, String> entry : keyRanges.entrySet()) {
            //				ReaderThread reader = new ReaderThread("Biocache", sourceDb, pageSize, queue, errorqueue,
//						entry.getKey(), entry.getValue(), cf, keySpace, map);
            readerExecutor.execute(readerBuilder.setStartKey(entry.getKey()).setEndKey(entry.getValue()).createReaderThread());
        }
        return readerExecutor;
    }

    private static Set<String> getTargetFieldSet(CassandraCluster cassandraCluster) {
        Set<String> fieldSet = new HashSet<>();
        ResultSet resultSet = cassandraCluster.getSession().execute("select * from system_schema.columns where keyspace_name='" + cassandraCluster.getKeySpace() + "' and table_name='" +
                cassandraCluster
                        .getColumnFamily() + "'");

        while (!resultSet.isExhausted()) {
            Row row = resultSet.one();
            fieldSet.add(row.getString("column_name"));
        }
        return fieldSet;
    }

    static Config readConfig(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Config config = null;
        config = mapper.readValue(new File(filePath), Config.class);
        log.info("Config is used:\n" + ReflectionToStringBuilder.toString(config, ToStringStyle.MULTI_LINE_STYLE));
        return config;
    }

    static void migrateCass1ToCass3(Config config) throws IOException, SolrServerException, InterruptedException, ParseException {
        final String startKey = config.getStartKey();
        final String endKey = config.getEndKey();
        final int pageSize = config.getPageSize();
        final int readThreads = config.getReadThreads();
        final int writeThreads = config.getWriteThreads();
        final Boolean sync = config.isSyncSchema();
        final String solrBase = config.getSolrBase();

        final String[] columnFamilies = config.getSource().get("columnFamily").split(",");
        final String keySpace = config.getSource().get("keySpace");
        final String clusterName = config.getSource().get("clusterName");
        final String sourcedb = config.getSource().get("db");
        final String[] targetdb = config.getTarget().get("db").split(",");


        final Map<String, String> keyRanges = new TreeMap<>();
        keyRanges.put(startKey, endKey);
        for (String columnFamily : columnFamilies) {
            switch (columnFamily) {
                case "occ":
                    final Map<String, String> occKeyRanges = getOccKeyRanges(startKey, endKey, readThreads, solrBase);
                    copyColumnFmaily(clusterName, columnFamily, keySpace, occKeyRanges, new OccMapper(keySpace, columnFamily), sourcedb,
                            targetdb, readThreads, writeThreads, pageSize, solrBase, sync);
                    break;
                case "loc":
                    copyColumnFmaily(clusterName, columnFamily, keySpace, keyRanges, new LocMapper(keySpace, columnFamily), sourcedb,
                            targetdb, readThreads, writeThreads, pageSize, solrBase, sync);
                    break;
                case "attr":
                    copyColumnFmaily(clusterName, columnFamily, keySpace, keyRanges, new AttrMapper(keySpace, columnFamily), sourcedb,
                            targetdb, readThreads, writeThreads, pageSize, solrBase, sync);
                    break;
                case "qid":
                    copyColumnFmaily(clusterName, columnFamily, keySpace, keyRanges, new QidMapper(keySpace, columnFamily), sourcedb,
                            targetdb, readThreads, writeThreads, pageSize, solrBase, sync);
                    break;
                case "qa":
                    copyColumnFmaily(clusterName, columnFamily, keySpace, keyRanges, new QaMapper(keySpace, columnFamily), sourcedb,
                            targetdb, readThreads, writeThreads, pageSize, solrBase, sync);
                    break;
                case "taxon":
                    copyColumnFmaily(clusterName, columnFamily, keySpace, keyRanges, new QaMapper(keySpace, columnFamily), sourcedb,
                            targetdb, readThreads, writeThreads, pageSize, solrBase, sync);
                    break;
                default:
                    throw new ParseException("Invalid name for columnFamily: " + columnFamily);
            }
        }

    }

    public static void main(String... args) throws Exception {

        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "trace");
        System.setProperty("org.slf4j.simpleLogger.showShortLogName", "true");
//        System.setProperty("org.slf4j.simpleLogger.log.com.datastax.driver.core", "ERROR");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", simpleDateFormat.toPattern());


        if (System.getProperty("org.slf4j.simpleLogger.defaultLogLevel") == null) {
            System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
        }

        final CommandLineParser parser = new DefaultParser();

        final Options options = new Options();
        options.addOption("h", "help", false, "prints this message.");
        options.addOption("c", "config", true, "The application config file path, defaults to '" + DEFAULT_APP_CONFIG + "'");

        try {
            // parse the command line arguments
            final CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                final HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("CMigrate", options);
                return;
            }
            String configFile = line.getOptionValue("config", DEFAULT_APP_CONFIG);
            Config config = readConfig(configFile);
            if (config.getSource().get("type").equalsIgnoreCase("cassandra1") &&
                    config.getTarget().get("type").equalsIgnoreCase("cassandra3")) {
                migrateCass1ToCass3(config);
            }

        } catch (ParseException exp) {
            System.out.println("Unexpected exception: " + exp.getMessage());
            final HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("CMigrate", options);
            throw exp;
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
            throw e;
        }
    }
}
