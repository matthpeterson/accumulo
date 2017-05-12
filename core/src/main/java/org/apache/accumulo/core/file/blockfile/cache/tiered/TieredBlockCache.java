package org.apache.accumulo.core.file.blockfile.cache.tiered;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TieredBlockCache implements BlockCache {
	
	public static final class Block implements CacheEntry {
	  private volatile byte[] buffer;
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
	private final IgniteCache<String, Block> cache;
	private final TieredBlockCacheConfiguration conf;
	private final AtomicLong hitCount = new AtomicLong(0);
	private final AtomicLong requestCount = new AtomicLong(0);

	
	public TieredBlockCache(TieredBlockCacheConfiguration conf, Ignite ignite) {
		this.conf = conf;
		this.cache = ignite.getOrCreateCache(conf.getConfiguration());
		LOG.info("Created {} cache with configuration {}", 
				conf.getConfiguration().getName(), conf.getConfiguration());
	}
	
	public void stop() {
		this.cache.close();
	}
	
	@Override
	public CacheEntry cacheBlock(String blockName, byte[] buf, boolean inMemory) {
		return cacheBlock(blockName, buf);
	}
	
	@Override
	public CacheEntry cacheBlock(String blockName, byte[] buf) {
		return this.cache.getAndPutIfAbsent(blockName, new Block(buf));
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
