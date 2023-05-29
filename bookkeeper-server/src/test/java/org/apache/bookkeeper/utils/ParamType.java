package org.apache.bookkeeper.utils;

public enum ParamType {
    VALID,
    INVALID,
    EMPTY,
    NULL,
    CORRECT,
    INCORRECT;

    public enum LedgerMetaType {
        ENS_NEG,
        ENS_ZERO,
        ENS_POS,
        PASSDIGEST_VALID,
        PASSDIGEST_INVALID,
        STATE_OPEN,
        STATE_CLOSED
    }
}