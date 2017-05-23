/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.file.blockfile.cache.tiered;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TieredBlockCache implements BlockCache {

  static final class Block implements CacheEntry {
    private byte[] buffer;
    private volatile Object index;

    public Block(byte[] buffer) {
      this.buffer = requireNonNull(buffer);
    }

    @Override
    public byte[] getBuffer() {
      return buffer;
    }

    @Override
    public Object getIndex() {
      return index;
    }

    @Override
    public void setIndex(Object index) {
      this.index = index;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(TieredBlockCache.class);
  private final IgniteCache<String,Block> cache;
  private final CacheMetrics metrics;
  private final TieredBlockCacheConfiguration conf;
  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong requestCount = new AtomicLong(0);
  private final ScheduledFuture<?> future;

  public TieredBlockCache(TieredBlockCacheConfiguration conf, Ignite ignite) {
    this.conf = conf;
    this.cache = ignite.getOrCreateCache(conf.getConfiguration());
    metrics = cache.localMxBean();
    LOG.info("Created {} cache with configuration {}", conf.getConfiguration().getName(), conf.getConfiguration());
    this.future = TieredBlockCacheManager.SCHEDULER.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        LOG.info(cache.localMetrics().toString());
        LOG.info(cache.getName() + " entries, on-heap: " + getOnHeapEntryCount() + ", off-heap: " + getOffHeapEntryCount());
      }
    }, TieredBlockCacheManager.STAT_INTERVAL, TieredBlockCacheManager.STAT_INTERVAL, TimeUnit.SECONDS);
  }

  public void stop() {
    this.future.cancel(false);
    this.cache.close();
    this.cache.destroy();
  }

  public IgniteCache<String,Block> getInternalCache() {
    return this.cache;
  }

  public long getOnHeapEntryCount() {
    return this.cache.localSizeLong(CachePeekMode.ONHEAP);
  }

  public long getOffHeapEntryCount() {
    return this.cache.localSizeLong(CachePeekMode.OFFHEAP);
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf, boolean inMemory) {
    return cacheBlock(blockName, buf);
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf) {
    return this.cache.getAndPut(blockName, new Block(buf));
  }

  @Override
  public CacheEntry getBlock(String blockName) {
    this.requestCount.incrementAndGet();
    Block b = this.cache.get(blockName);
    if (null != b) {
      this.hitCount.incrementAndGet();
    }
    return b;
  }

  @Override
  public long getMaxSize() {
    return this.conf.getMaxSize();
  }

  @Override
  public long getMaxHeapSize() {
    return this.conf.getMaxSize();
  }

  public CacheMetrics getCacheMetrics() {
    return this.metrics;
  }

  @Override
  public Stats getStats() {
    return new Stats() {
      @Override
      public long hitCount() {
        return hitCount.get();
      }

      @Override
      public long requestCount() {
        return requestCount.get();
      }
    };
  }

}
