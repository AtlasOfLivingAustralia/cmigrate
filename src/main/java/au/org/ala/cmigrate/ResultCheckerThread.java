package au.org.ala.cmigrate;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Mahmoud Sadeghi on 8/5/17.
 */
public class ResultCheckerThread implements Runnable {

    private final Object sentinel;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final BlockingQueue<Map<String, String>> recordQueue;
    private final BlockingQueue<ResultRecordPair> resultQueue;
    private final BlockingQueue<ErrorRecord> errorQueue;
    private final BlockingQueue<ResultRecordPair> retryQueue;
    private final AtomicInteger writtenRowCount;
    private final AtomicInteger verifiedRowCount;
    private final int MAX_RETRY_COUNT = 10;



    public ResultCheckerThread(BlockingQueue<ResultRecordPair> resultQueue, BlockingQueue<Map<String, String>> recordQueue, BlockingQueue<ErrorRecord> errorQueue, BlockingQueue<ResultRecordPair> retryQueue, Object
            sentinel, AtomicInteger writtenRowCount, AtomicInteger verifiedRowCount) {
        this.sentinel = sentinel;
        this.recordQueue = recordQueue;
        this.resultQueue = resultQueue;
        this.writtenRowCount = writtenRowCount;
        this.errorQueue = errorQueue;
        this.verifiedRowCount = verifiedRowCount;
        this.retryQueue = retryQueue;
    }


    private void verifyResult(ResultSetFuture result, int verificationRetryCount, int timeout, TimeUnit timeUnit) throws ExecutionException, TimeoutException, InterruptedException {
        int retryCount = 0;
        for (; retryCount < verificationRetryCount; retryCount++) {
            if (retryCount > 0) {
                log.debug("Retrying to check the result for {}th time.\t RESULT QUEUE# {}\t  RECORD QUEUE# {}\t  ERROR QUEUE# {}", retryCount, resultQueue.size(),
                        recordQueue.size(), errorQueue.size());
            }
            try {
                //verifying the result
                result.get(timeout, timeUnit);
                break;
            }  catch (ExecutionException|TimeoutException|InterruptedException exception) {
                log.debug("Exception has occurred", exception );
                if (retryCount == verificationRetryCount - 1)
                    throw  exception;
            }
        }
    }

    @Override
    public void run() {
        ResultRecordPair result = null;
        log.info("Thread started.");
        while (true) {
            try {
                Object obj = resultQueue.poll(1, TimeUnit.MINUTES);
                if (obj == null) {
                    log.info("Got null object, retrying ...", this.getClass().getSimpleName());
                    continue;
                } else if (obj == sentinel) {
                    log.info("{} exiting ...", this.getClass().getSimpleName());
                    break;
                }
                result = (ResultRecordPair) obj;

                if (verifiedRowCount.incrementAndGet() % 10000 == 0) {
                    log.info("Records write confirmed: {},  RESULT QUEUE# {}  RECORD QUEUE# {}  ERROR QUEUE# {}", verifiedRowCount.get(),resultQueue.size(),
                            recordQueue.size(), errorQueue.size());
                }

                try {
                    verifyResult(result.getResult(), 4, 10 , TimeUnit.SECONDS);
                    if (result.incrementAndGetRetryCount() >= MAX_RETRY_COUNT){
                        log.warn("Couldn't successfully add the record to the target db in {}th try. Adding it to the errorQueue. record:{}", result.getRetryCount(),
                                Joiner.on('\n').withKeyValueSeparator(" -> ").join(result.getRecord()));
                        writtenRowCount.decrementAndGet();
                        if (!errorQueue.offer(new ErrorRecord(result.getRecord(), result.getColumnFamily()+"_result_error_log"), 30, TimeUnit.SECONDS)){
                            log.error("Couldn't add the failed record to the errorQueue. " , Joiner.on('\n').withKeyValueSeparator(" -> ").join(result.getRecord()));
                        }
                    }

                } catch ( Exception e) {
                    log.warn("Adding the record to the retry queue to issue another insert command. retry attempt# {}  record:{}", result.incrementAndGetRetryCount(),
                                    Joiner.on('\n').withKeyValueSeparator(" -> ").join(result.getRecord()));
                    writtenRowCount.decrementAndGet();
                    if (!retryQueue.offer(result, 30, TimeUnit.SECONDS)){
                        log.error("Couldn't add the failed record to the retry queue. " , Joiner.on('\n').withKeyValueSeparator(" -> ").join(result.getRecord()));
                    }
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted exception: ", e);
            }

        }
        log.info("Thread ended.");
    }
}
