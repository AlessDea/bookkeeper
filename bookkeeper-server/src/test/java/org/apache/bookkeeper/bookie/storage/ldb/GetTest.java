package org.apache.bookkeeper.bookie.storage.ldb;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(value= Parameterized.class)
public class GetTest {

	private Object expected;
	private long ledId;
	private long entId;

	private static final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;


	@Parameterized.Parameters
	public static Collection<Object[]> getParameters(){
		return Arrays.asList(new Object[][]{
				// these three below are expected to throw an exception because of the ledgedId is less than zero
				{null, -1, -1},
				{null, -1, 0},
				{null, -1, 1},

				// these are expected to return a null ByteBuff because it doesn't exist
				{null, 0, -1},
				{null, 0, 0},
				{null, 0, 1},
				{null, 1, -1},
				{null, 1, 0},
				{null, 1, 1},
		});
	}

	public GetTest(Object expected, long ledId, long entId) {
		this.expected = expected;
		this.ledId = ledId;
		this.entId = entId;
	}


	@Test
	public void testGet(){

		ReadCache rc = new ReadCache(allocator, 1024);
		ByteBuf b = null;

		try {
			b = rc.get(ledId, entId);
		} catch (Exception e) {
			b = null;
		}

		assertEquals(expected, b);
	}

}
