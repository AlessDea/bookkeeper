package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;


@RunWith(Parameterized.class)
public class WriteCacheTest {
    
	private boolean expected;

    private long ledgerId;
    private long entryId;
    
    private long existingLedgerId;
    private long existingEntryId;
    
    private ByteBuf entry;
    private WriteCache cache = null;
    private long beforeCount;
    private boolean existsMaxSegmentSize; // è un valore che in alcuni test è disponibile, altri no.

    // Costruttore
    public WriteCacheTest(boolean expected, long ledgerId, long entryId, Integer entrySize, boolean existsMaxSegmentSize) {
        configure(expected, ledgerId, entryId, entrySize, existsMaxSegmentSize);
    }

    public void configure(boolean expected, long ledgerId, long entryId, Integer entrySize, boolean existsMaxSegmentSize) {
        this.existingLedgerId = 5;
        this.existingEntryId = 5;
    	this.expected = expected;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        if (entrySize != null) this.entry = Unpooled.wrappedBuffer(new byte[entrySize]);
        else this.entry = null;
        
        this.existsMaxSegmentSize = existsMaxSegmentSize; //serve nel setup per definire new writeCache
    }

    @Parameterized.Parameters
    public static Collection<?> getParameter() {
        return Arrays.asList(new Object[][] {
            // {expected, ledgerId, entryId, entrySize, maxSegmentSize}
    		
    		// Configurazioni valide
    		{true, 5, 5, 1024, false},
    		
    		// Configurazioni non valide
    		// entry null
    		{false, 5, 5, null, false},
    		
    		// entryId non esistente e altre possibili varianti
    		{false, 5, 4, 1024, false},
    		{false, 5, 4, null, false},
    		
    		// entryId negativo e altre possibili varianti
    		{false, 5, -1, 1024, false},
    		{false, 5, -1, null, false},
    		
    		// ledgerId non esistente e altre possibili varianti
    		{false, 4, 5, 1024, false},
    		{false, 4, 5, null, false},
    		{false, 4, 4, 1024, false},
    		{false, 4, 4, null, false},
    		{false, 4, -1, 1024, false},
    		{false, 4, -1, null, false},
    		
    		// ledgerId negativo e altre possibili varianti
    		{false, -1, 5, 1024, false},
    		{false, -1, 5, null, false},
    		{false, -1, 4, 1024, false},
    		{false, -1, 4, null, false},
    		{false, -1, -1, 1024, false},
    		{false, -1, -1, null, false},
            
    		// cache full
            {false, 5, 5, 2*1024, true},

    		// Altre configurazioni non valide
    		{false, 5, 5, 6*1024, true}, // cache full
    		{false, 5, 1, 2*1024, true}, // jacoco: maxSegSize - localOffset < size
    		{false, 5, -2, 1024, false}, // jacoco: currentLastEntryId > entryId
    		{false, -5, -2, 1024, false} // jacoco: currentLastEntryId > entryId
    		
        });
    }

    @Before
    public void setup(){
        if (existsMaxSegmentSize) {
            cache = new WriteCache(UnpooledByteBufAllocator.DEFAULT,4*1024,1024);  //MaxCacheSize   maxSegSize
        }
        else { 
            cache = new WriteCache(UnpooledByteBufAllocator.DEFAULT,10*1024);
        }

        if (entry != null) {
        	beforeCount = cache.count();
        	// nel metodo put di WriteCache, size = entry.readableBytes = writerIndex - readerIndex, 
        	// il secondo è sempre 0, il primo lo metto = capacità (writerIndex <= capacity). 
        	// fonte : https://netty.io/4.0/api/io/netty/buffer/ByteBuf.html
        	entry.writerIndex(entry.capacity()); 
        }
    }

    @Test
    public void TestWriteCache(){
        boolean actual;
        try {
            cache.put(ledgerId, entryId, entry);
            actual = cache.get(existingLedgerId, existingEntryId).equals(entry);
        } catch (Exception e) {
            actual = false;
        }
        if ((entry != null) && cache.count() <= beforeCount) actual = false;
        assertEquals(expected,actual);
    }

    @After
    public void teardown() {
        cache.clear();
        cache.close();
    }

}
