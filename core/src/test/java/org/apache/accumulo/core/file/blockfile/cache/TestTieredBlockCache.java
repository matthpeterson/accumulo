package org.apache.accumulo.core.file.blockfile.cache;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.blockfile.cache.tiered.TieredBlockCache;
import org.apache.accumulo.core.file.blockfile.cache.tiered.TieredBlockCacheManager;
import org.junit.Assert;
import org.junit.Test;

public class TestTieredBlockCache {
	
	private static final long BLOCKSIZE = 1024;
	private static final long MAXSIZE = 1024*100;
	
	private static class Holder {
		private final String name;
		private final byte[] buf;
		public Holder(String name, byte[] buf) {
			super();
			this.name = name;
			this.buf = buf;
		}
		public String getName() {
			return name;
		}
		public byte[] getBuf() {
			return buf;
		}
	}
	
	@Test
	public void testCacheCreation() throws Exception {
	    DefaultConfiguration dc = new DefaultConfiguration();
	    ConfigurationCopy cc = new ConfigurationCopy(dc);
	    cc.set(Property.TSERV_CACHE_FACTORY_IMPL, TieredBlockCacheManager.class.getName());
	    BlockCacheManager manager = BlockCacheManager.getInstance(cc);
	    cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(BLOCKSIZE));
	    cc.set(Property.TSERV_DATACACHE_SIZE, Long.toString(MAXSIZE));
	    cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(MAXSIZE));
	    cc.set(Property.TSERV_SUMMARYCACHE_SIZE, Long.toString(MAXSIZE));
	    manager.start(cc);
	    TieredBlockCache cache = (TieredBlockCache) manager.getBlockCache(CacheType.DATA);
	    Holder[] blocks = generateRandomBlocks(100, BLOCKSIZE);
	    // Confirm empty
	    for (Holder h : blocks) {
	      Assert.assertNull(cache.getBlock(h.getName()));
	    }
	    // Add blocks
	    for (Holder h : blocks) {
	      cache.cacheBlock(h.getName(), h.getBuf());
	    }
	    // Check if all blocks are properly cached and retrieved
	    for (Holder h : blocks) {
	      CacheEntry ce = cache.getBlock(h.getName());
	      Assert.assertTrue(ce != null);
	      Assert.assertEquals(ce.getBuffer().length, h.getBuf().length);
	    }

	    manager.stop();
	}
	
    private Holder[] generateRandomBlocks(int numBlocks, long maxSize) {
      Holder[] blocks = new Holder[numBlocks];
      Random r = new Random();
      for (int i = 0; i < numBlocks; i++) {
        blocks[i] = new Holder("block" + i, Integer.toString(r.nextInt((int) maxSize) + 1).getBytes(StandardCharsets.UTF_8));
      }
      return blocks;
    }

}
