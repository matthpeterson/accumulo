package org.apache.accumulo.core.file.blockfile.cache.tiered;

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
	
	private final CacheConfiguration<String, Block> configuration;

	public TieredBlockCacheConfiguration(AccumuloConfiguration conf, CacheType type) {
	  super(conf, type, TieredBlockCacheManager.PROPERTY_PREFIX);

	  configuration = new CacheConfiguration<>();
	  configuration.setName(type.name());
	  configuration.setCacheMode(CacheMode.LOCAL);
	  configuration.setOnheapCacheEnabled(true);
	  LruEvictionPolicy<String, Block> ePolicy = new LruEvictionPolicy<>();
	  ePolicy.setMaxSize((int) (0.75 * this.getMaxSize()));
	  ePolicy.setMaxMemorySize(this.getMaxSize());
	  configuration.setEvictionPolicy(ePolicy);
	  configuration.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(Duration.ONE_HOUR));
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
