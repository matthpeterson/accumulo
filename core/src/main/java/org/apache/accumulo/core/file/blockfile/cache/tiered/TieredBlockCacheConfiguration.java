package org.apache.accumulo.core.file.blockfile.cache.tiered;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;
import org.apache.accumulo.core.file.blockfile.cache.tiered.TieredBlockCache.Block;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;

public class TieredBlockCacheConfiguration extends BlockCacheConfiguration {
	
	public static final String CACHE_EXPIRATION_TIME = TieredBlockCacheManager.TIERED_PROPERTY_BASE + "expiration.time";
	public static final String CACHE_EXPIRATION_TIME_UNITS = TieredBlockCacheManager.TIERED_PROPERTY_BASE + "expiration.time_units";
	
	private static final String DEFAULT_CACHE_EXPIRATION_TIME_UNITS = "HOURS";
	private static final long DEFAULT_CACHE_EXPIRATION_TIME = 1;
	
	private final CacheConfiguration<String, Block> configuration;

	public TieredBlockCacheConfiguration(AccumuloConfiguration conf, CacheType type) {
	  super(conf, type, TieredBlockCacheManager.PROPERTY_PREFIX);

	  String unit = Optional.ofNullable(conf.get(CACHE_EXPIRATION_TIME_UNITS)).orElse(DEFAULT_CACHE_EXPIRATION_TIME_UNITS);
	  long time = Optional.ofNullable(conf.get(CACHE_EXPIRATION_TIME)).map(Long::valueOf).filter(f -> f > 0).orElse(DEFAULT_CACHE_EXPIRATION_TIME);
	  
	  configuration = new CacheConfiguration<>();
	  configuration.setName(type.name());
	  configuration.setCacheMode(CacheMode.LOCAL);
	  configuration.setOnheapCacheEnabled(true);
	  LruEvictionPolicy<String, Block> ePolicy = new LruEvictionPolicy<>();
	  ePolicy.setMaxSize((int) (0.75 * this.getMaxSize()));
	  ePolicy.setMaxMemorySize(this.getMaxSize());
	  configuration.setEvictionPolicy(ePolicy);
	  configuration.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.valueOf(unit), time)));
	  configuration.setStatisticsEnabled(true);
	}

	public CacheConfiguration<String, Block> getConfiguration() {
	  return configuration;
	}

	@Override
	public String toString() {
	  return this.configuration.toString();
	}

}
