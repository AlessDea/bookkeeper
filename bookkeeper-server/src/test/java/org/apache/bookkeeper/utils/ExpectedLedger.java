package org.apache.bookkeeper.utils;

import lombok.Data;

@Data
public class ExpectedLedger {
	
    private long ledgerId;

    public ExpectedLedger(long ledgerId) {
        this.ledgerId = ledgerId;
    }
}
	
