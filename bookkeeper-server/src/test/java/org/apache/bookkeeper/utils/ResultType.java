package org.apache.bookkeeper.utils;

public enum ResultType {
    ILLEGAL_ARG_ERR, // IllegalArgumentException
    UNEXPECTED_ERR, // UnexpectedConditionException
    BK_ERR, // BKException
    OK,
    NULL_PTR_ERR
}
