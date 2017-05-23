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
import java.util.concurrent.TimeUnit;

import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;

public class TieredBlockCacheConfiguration extends BlockCacheConfiguration {

  public static final String CACHE_EXPIRATION_TIME = TieredBlockCacheManager.TIERED_PROPERTY_BASE + "expiration.time";
  public static final String CACHE_EXPIRATION_TIME_UNITS = TieredBlockCacheManager.TIERED_PROPERTY_BASE + "expiration.time_units";

  private static final String DEFAULT_CACHE_EXPIRATION_TIME_UNITS = "HOURS";
  private static final long DEFAULT_CACHE_EXPIRATION_TIME = 1;

  private final CacheConfiguration<String,BinaryObject> configuration;

  public TieredBlockCacheConfiguration(AccumuloConfiguration conf, CacheType type) {
    super(conf, type, TieredBlockCacheManager.PROPERTY_PREFIX);

    String unit = Optional.ofNullable(conf.get(CACHE_EXPIRATION_TIME_UNITS)).orElse(DEFAULT_CACHE_EXPIRATION_TIME_UNITS);
    long time = Optional.ofNullable(conf.get(CACHE_EXPIRATION_TIME)).map(Long::valueOf).filter(f -> f > 0).orElse(DEFAULT_CACHE_EXPIRATION_TIME);

    configuration = new CacheConfiguration<>();
    configuration.setName(type.name());
    configuration.setCacheMode(CacheMode.LOCAL);
    configuration.setOnheapCacheEnabled(true);
    LruEvictionPolicy<String,BinaryObject> ePolicy = new LruEvictionPolicy<>();
    ePolicy.setMaxSize((int) (0.75 * this.getMaxSize()));
    ePolicy.setMaxMemorySize(this.getMaxSize());
    configuration.setEvictionPolicy(ePolicy);
    configuration.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.valueOf(unit), time)));
    configuration.setStatisticsEnabled(true);
    configuration.setCopyOnRead(false);
  }

  public CacheConfiguration<String,BinaryObject> getConfiguration() {
    return configuration;
  }

  @Override
  public String toString() {
    return this.configuration.toString();
  }

}
