package com.flipkart.dsp.sg.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public class FutureUtils {
    public static <T> Set<T> getEntitiesFromFutures(List<CompletableFuture<T>> cfs) throws ExecutionException, CompletionException {
        CompletableFuture.allOf(cfs.toArray(new CompletableFuture[0]));

        Set<T> tables = new HashSet<>();
        for (CompletableFuture<T> cf : cfs) {
            try {
                tables.add(cf.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        return tables;
    }
}
