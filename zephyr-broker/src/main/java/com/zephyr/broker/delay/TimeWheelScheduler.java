package com.zephyr.broker.delay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Time Wheel based delay scheduler
 * Efficiently handles large numbers of delayed tasks
 */
public class TimeWheelScheduler {

    private static final Logger logger = LoggerFactory.getLogger(TimeWheelScheduler.class);

    private final int wheelSize;
    private final long tickMs;
    private final long intervalMs;

    private final TimerTaskList[] wheel;
    private final DelayQueue<Delayed> delayQueue;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile long currentTime;
    private Thread workerThread;

    public TimeWheelScheduler(int wheelSize, long tickMs) {
        this.wheelSize = wheelSize;
        this.tickMs = tickMs;
        this.intervalMs = tickMs * wheelSize;

        this.wheel = new TimerTaskList[wheelSize];
        this.delayQueue = new DelayQueue<>();
        this.currentTime = System.currentTimeMillis();

        // Initialize wheel buckets
        for (int i = 0; i < wheelSize; i++) {
            wheel[i] = new TimerTaskList();
        }
    }

    /**
     * Start the time wheel scheduler
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            workerThread = new Thread(this::run, "TimeWheelScheduler");
            workerThread.setDaemon(true);
            workerThread.start();
            logger.info("TimeWheelScheduler started with wheel size: {}, tick: {}ms", wheelSize, tickMs);
        }
    }

    /**
     * Shutdown the time wheel scheduler
     */
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            if (workerThread != null) {
                workerThread.interrupt();
                try {
                    workerThread.join(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            logger.info("TimeWheelScheduler shut down");
        }
    }

    /**
     * Schedule a task for delayed execution
     *
     * @param task the task to execute
     * @param delayMs delay in milliseconds
     * @return true if scheduled successfully
     */
    public boolean schedule(DelayTask task, long delayMs) {
        if (!started.get()) {
            logger.warn("TimeWheelScheduler not started");
            return false;
        }

        long executeTime = System.currentTimeMillis() + delayMs;
        return addTimerTask(new TimerTask(task, executeTime));
    }

    /**
     * Schedule a task for execution at specific time
     *
     * @param task the task to execute
     * @param executeTime execution time in milliseconds
     * @return true if scheduled successfully
     */
    public boolean scheduleAt(DelayTask task, long executeTime) {
        if (!started.get()) {
            logger.warn("TimeWheelScheduler not started");
            return false;
        }

        return addTimerTask(new TimerTask(task, executeTime));
    }

    private boolean addTimerTask(TimerTask timerTask) {
        long executeTime = timerTask.getExecuteTime();
        long delay = executeTime - currentTime;

        if (delay < tickMs) {
            // Execute immediately
            timerTask.getTask().execute();
            return true;
        } else if (delay < intervalMs) {
            // Add to current wheel
            long bucketIndex = (executeTime / tickMs) % wheelSize;
            TimerTaskList bucket = wheel[(int) bucketIndex];
            bucket.add(timerTask);

            // Set bucket expiration if not set
            if (bucket.setExpiration(executeTime - (executeTime % tickMs))) {
                delayQueue.offer(bucket);
            }
            return true;
        } else {
            // Would need multiple levels of time wheels for very long delays
            // For simplicity, just use delay queue for long delays
            delayQueue.offer(new SingleTaskList(timerTask));
            return true;
        }
    }

    private void run() {
        while (started.get() && !Thread.currentThread().isInterrupted()) {
            try {
                advanceClock();
            } catch (InterruptedException e) {
                logger.info("TimeWheelScheduler worker thread interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in TimeWheelScheduler worker thread", e);
            }
        }
    }

    private void advanceClock() throws InterruptedException {
        Delayed delayed = delayQueue.take(); // This will block until a bucket expires

        if (delayed != null) {
            if (delayed instanceof TimerTaskList) {
                TimerTaskList bucket = (TimerTaskList) delayed;
                // Update current time
                currentTime = bucket.getExpiration();

                // Execute all tasks in the bucket
                bucket.flush(this::executeTask);
            } else if (delayed instanceof SingleTaskList) {
                SingleTaskList singleTask = (SingleTaskList) delayed;
                // Update current time
                currentTime = singleTask.getExpiration();

                // Execute the single task
                singleTask.flush(this::executeTask);
            }
        }
    }

    private void executeTask(TimerTask timerTask) {
        try {
            long executeTime = timerTask.getExecuteTime();
            if (executeTime <= System.currentTimeMillis()) {
                timerTask.getTask().execute();
            } else {
                // Re-add to wheel if execution time is in the future
                addTimerTask(timerTask);
            }
        } catch (Exception e) {
            logger.error("Error executing delayed task", e);
        }
    }

    /**
     * Interface for delayed tasks
     */
    public interface DelayTask {
        void execute();
        String getTaskId();
    }

    /**
     * Timer task wrapper
     */
    private static class TimerTask {
        private final DelayTask task;
        private final long executeTime;
        private TimerTask next;
        private TimerTask prev;

        public TimerTask(DelayTask task, long executeTime) {
            this.task = task;
            this.executeTime = executeTime;
        }

        public DelayTask getTask() { return task; }
        public long getExecuteTime() { return executeTime; }
    }

    /**
     * Timer task list (bucket in time wheel)
     */
    private static class TimerTaskList implements Delayed {
        private final TimerTask root = new TimerTask(null, -1);
        private volatile long expiration = -1;
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        {
            root.next = root;
            root.prev = root;
        }

        public boolean setExpiration(long expiration) {
            this.expiration = expiration;
            return true;
        }

        public long getExpiration() {
            return expiration;
        }

        public void add(TimerTask timerTask) {
            lock.writeLock().lock();
            try {
                if (timerTask.next == null && timerTask.prev == null) {
                    TimerTask tail = root.prev;
                    timerTask.next = root;
                    timerTask.prev = tail;
                    tail.next = timerTask;
                    root.prev = timerTask;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void flush(java.util.function.Consumer<TimerTask> executor) {
            lock.writeLock().lock();
            try {
                TimerTask current = root.next;
                while (current != root) {
                    TimerTask next = current.next;
                    current.next = null;
                    current.prev = null;
                    executor.accept(current);
                    current = next;
                }
                root.next = root;
                root.prev = root;
                expiration = -1;
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(Math.max(0, expiration - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if (o instanceof TimerTaskList) {
                return Long.compare(expiration, ((TimerTaskList) o).expiration);
            }
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }
    }

    /**
     * Single task list for very long delays
     */
    private static class SingleTaskList implements Delayed {
        private final TimerTask timerTask;

        public SingleTaskList(TimerTask timerTask) {
            this.timerTask = timerTask;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(Math.max(0, timerTask.executeTime - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }

        public void flush(java.util.function.Consumer<TimerTask> executor) {
            executor.accept(timerTask);
        }

        public long getExpiration() {
            return timerTask.executeTime;
        }
    }
}