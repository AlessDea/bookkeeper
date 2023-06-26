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

import static org.junit.Assert.assertEquals;

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
				{true, 123, 0},
				{false, 123, 1},
				{false, 10000, 0},
				{false, 10000, 1},
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
		if(b != null)
			assertEquals(entry, b);

	}

}
