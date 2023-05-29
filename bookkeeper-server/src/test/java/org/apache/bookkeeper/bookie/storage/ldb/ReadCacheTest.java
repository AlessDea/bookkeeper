package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.bookkeeper.bookie.storage.utils.ParamTypeReadCache;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

@RunWith(Parameterized.class)
public class ReadCacheTest {
	
	private boolean expected;
	
	private long realLedgerId;
	private long realEntryId;
	private long ledgerIdGet;
	private long entryIdGet;
	
	private ReadCache cache = null;
	private ByteBuf entry; 
	
	private static Random random;
	
	@BeforeClass
	public static void setUp() {
		random = new Random();
	}
	
	// Costruttore
	public ReadCacheTest(boolean expected, ParamTypeReadCache ledgerIdType, ParamTypeReadCache entryIdType) {
		configure(expected, ledgerIdType, entryIdType);
	}
	
	private void configure(boolean expected, ParamTypeReadCache ledgerIdType, ParamTypeReadCache entryIdType) {
		this.expected = expected;
		configureLedgerId(ledgerIdType);
		configureEntryId(entryIdType);
	}
	
	private void configureLedgerId(ParamTypeReadCache ledgerIdType) {
		switch (ledgerIdType) {
			case POSITIVE_EXISTING_LEDGER_ID:
				this.realLedgerId = (long) random.nextInt(1000 + 1); // [ min = 0, max = 1000]
				this.ledgerIdGet = this.realLedgerId;
				break;
			case POSITIVE_NON_EXISTING_LEDGER_ID:
				this.realLedgerId = (long) random.nextInt(1000 + 1); // [ min = 0, max = 1000]
				this.ledgerIdGet = this.realLedgerId + 1;
				break;
			case NEGATIVE_LEDGER_ID:
				this.ledgerIdGet = -1;
				break;
			default:
				break;
		}
	}
	
	private void configureEntryId(ParamTypeReadCache entryIdType) { 
		switch (entryIdType) {
			case POSITIVE_EXISTING_ENTRY_ID:
				this.realEntryId = (long) random.nextInt(500 + 1); // [min = 0, max = 500]
				this.entryIdGet = this.realEntryId;
				break;
			case POSITIVE_NON_EXISTING_ENTRY_ID:
				this.realEntryId = (long) random.nextInt(500 + 1); // [min = 0, max = 500]
				this.entryIdGet = this.realEntryId + 1; 
				break;
			case NEGATIVE_ENTRY_ID:
				this.entryIdGet = -1; 
				break;
			default:
				break;
		}
	}

	@Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        return Arrays.asList(new Object[][]{
            // {expected,  ledgerIdGet,  entryIdGet}
            
        	// Configurazione valida
        	{true, ParamTypeReadCache.POSITIVE_EXISTING_LEDGER_ID, ParamTypeReadCache.POSITIVE_EXISTING_ENTRY_ID},
            
        	// Configurazioni non valide
        	{false, ParamTypeReadCache.POSITIVE_EXISTING_LEDGER_ID, ParamTypeReadCache.POSITIVE_NON_EXISTING_ENTRY_ID},
        	{false, ParamTypeReadCache.POSITIVE_EXISTING_LEDGER_ID, ParamTypeReadCache.NEGATIVE_ENTRY_ID},

        	{false, ParamTypeReadCache.POSITIVE_NON_EXISTING_LEDGER_ID, ParamTypeReadCache.POSITIVE_EXISTING_ENTRY_ID},
        	{false, ParamTypeReadCache.POSITIVE_NON_EXISTING_LEDGER_ID, ParamTypeReadCache.POSITIVE_NON_EXISTING_ENTRY_ID},
        	{false, ParamTypeReadCache.POSITIVE_NON_EXISTING_LEDGER_ID, ParamTypeReadCache.NEGATIVE_ENTRY_ID},
        	
        	{false, ParamTypeReadCache.NEGATIVE_LEDGER_ID, ParamTypeReadCache.POSITIVE_EXISTING_ENTRY_ID},
        	{false, ParamTypeReadCache.NEGATIVE_LEDGER_ID, ParamTypeReadCache.POSITIVE_NON_EXISTING_ENTRY_ID},
        	{false, ParamTypeReadCache.NEGATIVE_LEDGER_ID, ParamTypeReadCache.NEGATIVE_ENTRY_ID}
        	
        });
    }
	
	@Before
	public void setUpReadCache() {
		// Inizializzazione della cache
		cache = new ReadCache(UnpooledByteBufAllocator.DEFAULT, 10 * 1024);
		entry = Unpooled.wrappedBuffer(new byte[1024]);
		cache.put(realLedgerId, realEntryId, entry);
	}
	
	@Test
	public void readCacheTest() {
		boolean actual;
		try {
			actual = cache.get(ledgerIdGet, entryIdGet).equals(entry);
		} catch (Exception e) {
			actual = false;
		}
		assertEquals(expected, actual);
	}
	
	@After
	public void tearDown() {
		cache.close();
	}
	
}
