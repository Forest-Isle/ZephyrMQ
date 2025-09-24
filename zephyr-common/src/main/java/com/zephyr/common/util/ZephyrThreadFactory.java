package com.zephyr.common.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ZephyrThreadFactory implements ThreadFactory {

    private final AtomicInteger threadIndex = new AtomicInteger(0);
    private final String threadNamePrefix;
    private final boolean daemon;

    public ZephyrThreadFactory(String threadNamePrefix) {
        this(threadNamePrefix, false);
    }

    public ZephyrThreadFactory(String threadNamePrefix, boolean daemon) {
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, threadNamePrefix + "_" + threadIndex.incrementAndGet());
        thread.setDaemon(daemon);
        return thread;
    }
}