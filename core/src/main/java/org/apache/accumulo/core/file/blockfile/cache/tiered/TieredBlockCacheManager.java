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

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheManager;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TieredBlockCacheManager extends BlockCacheManager {

  private static final Logger LOG = LoggerFactory.getLogger(TieredBlockCacheManager.class);

  static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1, new NamingThreadFactory("TieredBlockCacheStats"));
  static final int STAT_INTERVAL = 60;

  public static final String PROPERTY_PREFIX = "tiered";
  public static final String TIERED_PROPERTY_BASE = BlockCacheConfiguration.CACHE_PROPERTY_BASE + PROPERTY_PREFIX + ".";

  public static final String OFF_HEAP_MAX_SIZE_PROPERTY = TIERED_PROPERTY_BASE + "off-heap.max.size";
  public static final String OFF_HEAP_BLOCK_SIZE_PROPERTY = TIERED_PROPERTY_BASE + "off-heap.block.size";

  private static final long OFF_HEAP_MIN_SIZE = 10 * 1024 * 1024;
  private static final long OFF_HEAP_MAX_SIZE_DEFAULT = 512 * 1024 * 1024;
  private static final int OFF_HEAP_BLOCK_SIZE_DEFAULT = 16 * 1024;
  private static final String OFF_HEAP_CONFIG_NAME = "OFF_HEAP_MEMORY";

  private Ignite IGNITE;
  private ScheduledFuture<?> future;

  @Override
  public void start(AccumuloConfiguration conf) {

    long offHeapMaxSize = Optional.ofNullable(conf.get(OFF_HEAP_MAX_SIZE_PROPERTY)).map(Long::valueOf).filter(f -> f > 0).orElse(OFF_HEAP_MAX_SIZE_DEFAULT);
    final int offHeapBlockSize = Optional.ofNullable(conf.get(OFF_HEAP_BLOCK_SIZE_PROPERTY)).map(Integer::valueOf).filter(f -> f > 0)
        .orElse(OFF_HEAP_BLOCK_SIZE_DEFAULT);

    if (offHeapMaxSize < OFF_HEAP_MIN_SIZE) {
      LOG.warn("Off heap max size setting too low, overriding to minimum of 10MB");
      offHeapMaxSize = OFF_HEAP_MIN_SIZE;
    }
    // Ignite configuration.
    IgniteConfiguration cfg = new IgniteConfiguration();
    cfg.setDaemon(true);

    // Global Off-Heap Page memory configuration.
    MemoryConfiguration memCfg = new MemoryConfiguration();
    memCfg.setPageSize(offHeapBlockSize);

    MemoryPolicyConfiguration plCfg = new MemoryPolicyConfiguration();
    plCfg.setName(OFF_HEAP_CONFIG_NAME);
    plCfg.setInitialSize(offHeapMaxSize);
    plCfg.setMaxSize(offHeapMaxSize);
    plCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU);
    plCfg.setEvictionThreshold(0.9);
    plCfg.setEmptyPagesPoolSize((int) (offHeapMaxSize / offHeapBlockSize / 10) - 1);
    plCfg.setMetricsEnabled(true);

    memCfg.setMemoryPolicies(plCfg); // apply custom memory policy
    memCfg.setDefaultMemoryPolicyName(OFF_HEAP_CONFIG_NAME);

    cfg.setMemoryConfiguration(memCfg); // apply off-heap memory configuration
    LOG.info("Starting Ignite with configuration {}", cfg.toString());
    IGNITE = Ignition.start(cfg);

    super.start(conf);

    future = SCHEDULER.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        IGNITE.memoryMetrics().forEach(m -> {
          ToStringBuilder builder = new ToStringBuilder(m);
          builder.append("memory region name", m.getName());
          builder.append(" page allocation rate", m.getAllocationRate());
          builder.append(" page eviction rate", m.getEvictionRate());
          builder.append(" total allocated pages", m.getTotalAllocatedPages());
          builder.append(" page free space %", m.getPagesFillFactor());
          builder.append(" large entry fragmentation %", m.getLargeEntriesPagesPercentage());
          LOG.info(builder.toString());
        });
      }
    }, STAT_INTERVAL, STAT_INTERVAL, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    for (CacheType type : CacheType.values()) {
      TieredBlockCache cache = (TieredBlockCache) this.getBlockCache(type);
      if (null != cache) {
        cache.stop();
      }
    }
    if (null != future) {
      future.cancel(false);
    }
    IGNITE.close();
    super.stop();
  }

  @Override
  protected BlockCache createCache(AccumuloConfiguration conf, CacheType type) {
    return new TieredBlockCache(new TieredBlockCacheConfiguration(conf, type), IGNITE);
  }

}
