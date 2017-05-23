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

import java.lang.ref.SoftReference;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.apache.accumulo.core.file.rfile.BlockIndex;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
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

    private BinaryObject binaryObject;

    private transient byte[] buffer;
    private transient BlockIndex idx;

    private LazyBlock(BinaryObject object, IgniteCache<String,BinaryObject> cache) {
      this.binaryObject = object;
      this.cache = cache;
    }

    @Override
    public byte[] getBuffer() {
      if (null == buffer) {
        buffer = this.binaryObject.field(BUFFER_FIELD);
      }
      return buffer;
    }

    @Override
    public Object getIndex() {
      if (null == idx && this.binaryObject.hasField(INDEX_FIELD)) {
        Object o = this.binaryObject.field(INDEX_FIELD);
        if (o instanceof BinaryObjectImpl) {
          BinaryObjectImpl b = (BinaryObjectImpl) o;
          idx = b.deserialize();
        } else if (o instanceof BlockIndex) {
          idx = (BlockIndex) o;
        } else {
          throw new RuntimeException("Unknown object type: " + o.getClass().getName());
        }
      }
      return new SoftReference<BlockIndex>(idx);
    }

    private void resetBinary(BinaryObject binary) {
      this.binaryObject = binary;
      this.buffer = null;
      this.idx = null;
    }

    @Override
    public void setIndex(final Object idx) {
      if (idx instanceof SoftReference<?>) {
        SoftReference<?> ref = (SoftReference<?>) idx;
        Object o = ref.get();
        if (o == null) {
          throw new IllegalStateException("Index is not set");
        }
        if (o instanceof BlockIndex) {
          final BlockIndex bi = (BlockIndex) o;
          final String key = this.binaryObject.field(KEY_FIELD);
          this.cache.invoke(key, new CacheEntryProcessor<String,BinaryObject,Void>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Void process(MutableEntry<String,BinaryObject> entry, Object... arguments) throws EntryProcessorException {
              // final BinaryObjectBuilder builder = igniteBinary.builder(LazyBlock.class.getName());
              // final String k = entry.getValue().field(KEY_FIELD);
              // final byte[] b = entry.getValue().field(BUFFER_FIELD);
              // builder.setField(KEY_FIELD, k);
              // builder.setField(BUFFER_FIELD, b);
              final BinaryObjectBuilder builder = entry.getValue().toBuilder();
              builder.setField(INDEX_FIELD, bi, BlockIndex.class);
              final BinaryObject update = builder.build();
              entry.setValue(update);
              resetBinary(update);
              return null;
            }
          });
        } else {
          throw new UnsupportedOperationException("Object is not a BlockIndex, is a: " + o.getClass().getName());
        }
      } else {
        throw new UnsupportedOperationException("Unhandled object type");
      }
    }

    /**
     * Convert a BinaryObject to a LazyBlock. Use this method instead of BinaryObject.deserialize as it sets the reference to the IgniteCache instance to use
     * when setIndex is invoked
     *
     * @param object
     *          {@link BinaryObject} representing this LazyBlock
     * @param cache
     *          reference to the {@link IgniteCache} whence the {@link BinaryObject} is stored
     * @return instance of {@link LazyBlock}
     */
    static LazyBlock fromIgniteBinaryObject(BinaryObject object, IgniteCache<String,BinaryObject> cache) {
      requireNonNull(object);
      requireNonNull(cache);
      return new LazyBlock(object, cache);
    }

    /**
     * Convert a byte array representing a block to a binary object to put into the ignite cache
     *
     * @param igniteBinary
     *          binary interface for {@link Ignite}
     * @param key
     *          they key that this block will be stored under in the cache
     * @param block
     *          the byte array representing the block
     * @return {@link BinaryObject} instance
     */
    static BinaryObject toIgniteBinaryObject(IgniteBinary igniteBinary, String key, byte[] block) {
      requireNonNull(key);
      requireNonNull(block);
      requireNonNull(igniteBinary);
      BinaryObjectBuilder builder = igniteBinary.builder(LazyBlock.class.getName());
      builder.setField(KEY_FIELD, key);
      builder.setField(BUFFER_FIELD, block);
      return builder.build();
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(TieredBlockCache.class);
  private final IgniteCache<String,BinaryObject> cache;
  private final IgniteBinary igniteBinary;
  private final CacheMetrics metrics;
  private final TieredBlockCacheConfiguration conf;
  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong requestCount = new AtomicLong(0);
  private final ScheduledFuture<?> future;

  public TieredBlockCache(TieredBlockCacheConfiguration conf, Ignite ignite) {
    this.conf = conf;
    this.cache = ignite.getOrCreateCache(conf.getConfiguration()).withKeepBinary();
    this.igniteBinary = ignite.binary();
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
    BinaryObject bo = this.cache.getAndPut(blockName, LazyBlock.toIgniteBinaryObject(igniteBinary, blockName, buf));
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
