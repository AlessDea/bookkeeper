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
public class HasEntryTest {

	private Object expected;
	private long ledId;
	private long entId;

	private static final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;


	@Parameterized.Parameters
	public static Collection<Object[]> getParameters(){
		return Arrays.asList(new Object[][]{
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
		});
	}

	public HasEntryTest(Object expected, long ledId, long entId) {
		this.expected = expected;
		this.ledId = ledId;
		this.entId = entId;
	}


	@Test
	public void testHasEntry(){

		ReadCache rc = new ReadCache(allocator, 1024);
		boolean ret;

		try {
			ret = rc.hasEntry(ledId, entId);
		} catch (Exception e) {
			ret = false;
		}

		assertEquals(expected, ret);
	}


}
