package com.flipkart.dsp.utils;

public enum HttpStatusCodeFamily {
    INFORMATIONAL,
    SUCCESSFUL,
    REDIRECTION,
    CLIENT_ERROR,
    SERVER_ERROR,
    OTHER;

    public static HttpStatusCodeFamily fromStatusCode(int code) {
        HttpStatusCodeFamily  family;
        switch (code / 100) {
            case 1:
                family = INFORMATIONAL;
                break;
            case 2:
                family = SUCCESSFUL;
                break;
            case 3:
                family = REDIRECTION;
                break;
            case 4:
                family = CLIENT_ERROR;
                break;
            case 5:
                family = SERVER_ERROR;
                break;
            default:
                family = OTHER;
        }
        return family;
    }
}
