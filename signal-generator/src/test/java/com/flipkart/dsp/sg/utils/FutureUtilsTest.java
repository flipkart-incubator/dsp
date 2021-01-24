package com.flipkart.dsp.sg.utils;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FutureUtils.class, CompletableFuture.class})
public class FutureUtilsTest {

    @Mock private Thread thread;
    @Mock private CompletableFuture<String> completableFuture;
    @Mock private CompletableFuture<Void> voidCompletableFuture;
    private List<CompletableFuture<String>> completableFutures = new ArrayList<>();

    @Before
    public void setUp() {
        PowerMockito.mockStatic(CompletableFuture.class);
        MockitoAnnotations.initMocks(this);
        completableFutures.add(completableFuture);
    }

    @Test
    public void testGetEntitiesFromFuturesSuccess() throws Exception {
        PowerMockito.when(CompletableFuture.allOf(CompletableFuture.allOf())).thenReturn(voidCompletableFuture);
        when(completableFuture.get()).thenThrow(new InterruptedException());

        Set<String> expected =  FutureUtils.getEntitiesFromFutures(completableFutures);
        assertNotNull(expected);
        PowerMockito.verifyStatic(CompletableFuture.class);
        CompletableFuture.allOf(CompletableFuture.allOf());
        verify(completableFuture).get();
    }

    @Test
    public void testGetEntitiesFromFuturesFailure() throws Exception {
        boolean isException = false;
        PowerMockito.when(CompletableFuture.allOf(CompletableFuture.allOf())).thenReturn(voidCompletableFuture);
        when(completableFuture.get()).thenThrow(new RuntimeException());

        try {
            FutureUtils.getEntitiesFromFutures(completableFutures);
        } catch (RuntimeException e) {
            isException = true;
        }

        assertTrue(isException);
        PowerMockito.verifyStatic(CompletableFuture.class);
        CompletableFuture.allOf(CompletableFuture.allOf());
        verify(completableFuture).get();
    }
}
