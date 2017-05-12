package org.apache.accumulo.core.file.blockfile.cache.tiered;

import java.util.Optional;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.BlockCacheManager;
import org.apache.accumulo.core.file.blockfile.cache.CacheType;
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
	
	public static final String PROPERTY_PREFIX = "tiered";
	
	private static final String TIERED_PROPERTY_BASE = BlockCacheManager.CACHE_PROPERTY_BASE + PROPERTY_PREFIX + ".";
	public static final String OFF_HEAP_MAX_SIZE_PROPERTY = TIERED_PROPERTY_BASE + "off-heap.max.size";
	public static final String OFF_HEAP_BLOCK_SIZE_PROPERTY = TIERED_PROPERTY_BASE + "off-heap.block.size";
	
	private static final long OFF_HEAP_MAX_SIZE_DEFAULT = 512 * 1024 * 1024;
	private static final int OFF_HEAP_BLOCK_SIZE_DEFAULT = 16 * 1024;

	private Ignite IGNITE;
	
	@Override
	public void start(AccumuloConfiguration conf) {
		
		final long offHeapMaxSize = Optional.ofNullable(conf.get(OFF_HEAP_MAX_SIZE_PROPERTY)).map(Long::valueOf).filter(f -> f > 0).orElse(OFF_HEAP_MAX_SIZE_DEFAULT);
		final int offHeapBlockSize = Optional.ofNullable(conf.get(OFF_HEAP_BLOCK_SIZE_PROPERTY)).map(Integer::valueOf).filter(f -> f > 0).orElse(OFF_HEAP_BLOCK_SIZE_DEFAULT);

		// Ignite configuration.
		IgniteConfiguration cfg = new IgniteConfiguration();
		cfg.setDaemon(true);
		
		// Global Off-Heap Page memory configuration.
		MemoryConfiguration memCfg = new MemoryConfiguration();
		memCfg.setPageSize(offHeapBlockSize);
		
		MemoryPolicyConfiguration plCfg = new MemoryPolicyConfiguration();
		plCfg.setInitialSize(offHeapMaxSize);
		plCfg.setMaxSize(offHeapMaxSize);
		plCfg.setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU);
		plCfg.setEvictionThreshold(0.9);

		memCfg.setMemoryPolicies(plCfg); //apply custom memory policy
		
		cfg.setMemoryConfiguration(memCfg); // apply off-heap memory configuration
		LOG.info("Starting Ignite with configuration {}", cfg.toString());
		IGNITE = Ignition.start(cfg);

		super.start(conf);
	}

	@Override
	public void stop() {
		for (CacheType type : CacheType.values()) {
		  TieredBlockCache cache = (TieredBlockCache) this.getBlockCache(type);
		  if (null != cache) {
			cache.stop();
		  }
		}
		IGNITE.close();
		super.stop();
	}

	@Override
	protected BlockCache createCache(AccumuloConfiguration conf, CacheType type) {
		return new TieredBlockCache(new TieredBlockCacheConfiguration(conf, type), IGNITE);
	}

}
