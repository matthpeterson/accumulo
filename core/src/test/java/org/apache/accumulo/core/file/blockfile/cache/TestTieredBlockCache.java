package org.apache.accumulo.core.file.blockfile.cache;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.blockfile.cache.tiered.TieredBlockCache;
import org.apache.accumulo.core.file.blockfile.cache.tiered.TieredBlockCacheManager;
import org.junit.Assert;
import org.junit.Test;

public class TestTieredBlockCache {
	
	private static final int BLOCKSIZE = 1024;
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
	    cc.set(Property.TSERV_CACHE_MANAGER_IMPL, TieredBlockCacheManager.class.getName());
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
	
	@Test
	public void testOffHeapBlockMigration() throws Exception {
	    DefaultConfiguration dc = new DefaultConfiguration();
	    ConfigurationCopy cc = new ConfigurationCopy(dc);
	    cc.set(Property.TSERV_CACHE_MANAGER_IMPL, TieredBlockCacheManager.class.getName());
	    cc.set("general.custom.cache.block.tiered.off-heap.max.size", Long.toString(10*1024*1024));
	    cc.set("general.custom.cache.block.tiered.off-heap.block.sizee", Long.toString(BLOCKSIZE));
	    BlockCacheManager manager = BlockCacheManager.getInstance(cc);
	    cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(BLOCKSIZE));
	    cc.set(Property.TSERV_DATACACHE_SIZE, "2048");
	    cc.set(Property.TSERV_INDEXCACHE_SIZE, "2048");
	    cc.set(Property.TSERV_SUMMARYCACHE_SIZE, "2048");
	    manager.start(cc);
	    TieredBlockCache cache = (TieredBlockCache) manager.getBlockCache(CacheType.DATA);
	    
	    // With this configuration we have an on-heap cache with a max size of 2K with 1K blocks
	    // and an off heap cache with a max size of 10MB with 1K blocks

	    for (Holder h : generateRandomBlocks(1, 1024)) {
	    	cache.cacheBlock(h.getName(), h.getBuf());
	    }
	    Assert.assertEquals(1, cache.getCacheMetrics().getCachePuts());
	    Assert.assertEquals(0, cache.getCacheMetrics().getOffHeapPuts());
	    
	    for (Holder h : generateRandomBlocks(1023, 1024)) {
	    	cache.cacheBlock(h.getName(), h.getBuf());
	    }
	    Assert.assertEquals(1, cache.getOnHeapEntryCount());
	    Assert.assertEquals(1023, cache.getOffHeapEntryCount());
	    
	    Assert.assertEquals(1, cache.getCacheMetrics().getSize());
	    Assert.assertEquals(1023, cache.getCacheMetrics().getOffHeapEntriesCount());
	    Assert.assertEquals(1024, cache.getCacheMetrics().getCachePuts());
	    Assert.assertEquals(0, cache.getCacheMetrics().getOffHeapPuts());
	    
	    manager.stop();

	}

	/**
	 * 
	 * @param numBlocks
	 *           number of blocks to create
	 * @param blockSize
	 *           number of bytes in each block
	 * @return
	 */
    private Holder[] generateRandomBlocks(int numBlocks, int blockSize) {
      byte[] b = new byte[blockSize];
      for (int x = 0; x < blockSize; x++) {
    	b[x] = '0';
      }
      Holder[] blocks = new Holder[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
    	byte[] buf = new byte[blockSize];
    	System.arraycopy(b, 0, buf, 0, blockSize);
        blocks[i] = new Holder("block" + i, buf);
      }
      return blocks;
    }

}
