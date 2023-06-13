package org.apache.bookkeeper.bookie.storage.ldb;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(value=Parameterized.class)
public class GetTest {

	private boolean expected;
	private long ledId;
	private long entId;

	private static final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

	ReadCache readCache;

	static ByteBuf entry = Unpooled.buffer(128);


	@Before
	public void configureValidCacheInfo(){

		readCache = new ReadCache(allocator, 1024);
		entry.writeBytes("Tutti a Istambul!".getBytes());
		readCache.put(123, 0, entry);

	}

	@Parameterized.Parameters
	public static Collection<Object[]> getParameters(){
		return Arrays.asList(new Object[][]{
				// false means that the expected value is false
				// true means that the expected value is a valid ByteBuf

				// these three below are expected to throw an exception because of the ledgedId is less than zero
				{false, -1, -1},
				{false, -1, 0},
				{false, -1, 1},

				// these are expected to return a null ByteBuff because it doesn't exist
				{false, 0, -1},
				{false, 0, 0},
				{false, 0, 1},

				{false, 1, -1},
				{false, 1, 0},
				{false, 1, 1},

				// this is expected to return a valid ByteBuf
				{true, 123, 0}
		});
	}

	public GetTest(boolean expected, long ledId, long entId) {
		this.expected = expected;
		this.ledId = ledId;
		this.entId = entId;
		//System.out.println("" + ledId + " " + entId);
	}


	@Test
	public void testGet(){

		ByteBuf b = null;

		boolean exp = false;


		try {
			b = readCache.get(this.ledId, this.entId);
		} catch (Exception e) {
			b = null;
		}

		if(b == null){
			exp = false;
		}else {
			exp = true;
		}

		assertEquals(this.expected, exp);

	}

}
