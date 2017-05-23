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

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TieredBlockCache implements BlockCache {

  /**
   * CacheEntry implementation that performs lazy deserialization based on an Ignite BinaryObject
   *
   */
  static final class LazyBlock implements CacheEntry {

    private static final String KEY_FIELD = "key";
    private static final String BUFFER_FIELD = "buffer";
    private static final String INDEX_FIELD = "index";

    private final IgniteCache<String,BinaryObject> cache;

    private BinaryObject binary;

    private transient byte[] buffer;
    private transient Object idx;

    private LazyBlock(BinaryObject binary, IgniteCache<String,BinaryObject> cache) {
      this.binary = binary;
      this.cache = cache;
    }

    @Override
    public byte[] getBuffer() {
      if (null == buffer) {
        buffer = this.binary.field(BUFFER_FIELD);
      }
      return buffer;
    }

    @Override
    public Object getIndex() {
      if (null == idx) {
        idx = this.binary.field(INDEX_FIELD);
      }
      return idx;
    }

    private void resetBinary(BinaryObject binary) {
      this.binary = binary;
      this.buffer = null;
      this.idx = null;
    }

    @Override
    public void setIndex(final Object idx) {
      String key = this.binary.field(KEY_FIELD);
      this.cache.invoke(key, new CacheEntryProcessor<String,BinaryObject,Void>() {
        private static final long serialVersionUID = 1L;

        @Override
        public Void process(MutableEntry<String,BinaryObject> entry, Object... arguments) throws EntryProcessorException {
          BinaryObjectBuilder builder = entry.getValue().toBuilder();
          builder.setField(INDEX_FIELD, idx);
          BinaryObject update = builder.build();
          entry.setValue(update);
          resetBinary(binary);
          return null;
        }
      });
    }

    /**
     * Convert a BinaryObject to a LazyBlock. Use this method instead of BinaryObject.deserialize as it sets the reference to the IgniteCache instance to use
     * when setIndex is invoked
     *
     * @param binary
     *          {@link BinaryObject} representing this LazyBlock
     * @param cache
     *          reference to the {@link IgniteCache} whence the {@link BinaryObject} is stored
     * @return instance of {@link LazyBlock}
     */
    static LazyBlock fromIgniteBinaryObject(BinaryObject binary, IgniteCache<String,BinaryObject> cache) {
      requireNonNull(binary);
      requireNonNull(cache);
      return new LazyBlock(binary, cache);
    }

    /**
     * Convert a byte array representing a block to a binary object to put into the ignite cache
     *
     * @param binary
     *          binary interface for {@link Ignite}
     * @param key
     *          they key that this block will be stored under in the cache
     * @param block
     *          the byte array representing the block
     * @return {@link BinaryObject} instance
     */
    static BinaryObject toIgniteBinaryObject(IgniteBinary binary, String key, byte[] block) {
      requireNonNull(key);
      requireNonNull(block);
      requireNonNull(binary);
      BinaryObjectBuilder builder = binary.builder(LazyBlock.class.getName());
      builder.setField(KEY_FIELD, key);
      builder.setField(BUFFER_FIELD, block);
      return builder.build();
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(TieredBlockCache.class);
  private final IgniteCache<String,BinaryObject> cache;
  private final IgniteBinary binary;
  private final CacheMetrics metrics;
  private final TieredBlockCacheConfiguration conf;
  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong requestCount = new AtomicLong(0);
  private final ScheduledFuture<?> future;

  public TieredBlockCache(TieredBlockCacheConfiguration conf, Ignite ignite) {
    this.conf = conf;
    this.cache = ignite.getOrCreateCache(conf.getConfiguration()).withKeepBinary();
    this.binary = ignite.binary();
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

  public IgniteCache<String,BinaryObject> getInternalCache() {
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
    BinaryObject bo = this.cache.getAndPut(blockName, LazyBlock.toIgniteBinaryObject(binary, blockName, buf));
    if (null != bo) {
      return LazyBlock.fromIgniteBinaryObject(bo, this.cache);
    } else {
      return null;
    }
  }

  @Override
  public CacheEntry getBlock(String blockName) {
    this.requestCount.incrementAndGet();
    BinaryObject bo = this.cache.get(blockName);
    if (null != bo) {
      this.hitCount.incrementAndGet();
      return LazyBlock.fromIgniteBinaryObject(bo, this.cache);
    }
    return null;
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
