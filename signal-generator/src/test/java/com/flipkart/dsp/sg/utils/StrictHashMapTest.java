package com.flipkart.dsp.sg.utils;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;

/**
 * +
 */
public class StrictHashMapTest {

    private StrictHashMap strictHashMap;

    @Before
    public void setUp() {
        this.strictHashMap = spy(new StrictHashMap());
    }

    @Test
    public void testSuccess() {
        String key = "key", value = "value";
        strictHashMap.put(key, value);
        String expected =  strictHashMap.get(key).toString();
        assertEquals(expected, value);
    }

    @Test
    public void testGetFailure() {
        String key = "key";
        boolean isException = false;

        try {
            strictHashMap.get(key).toString();
        } catch (StrictHashMap.StrictHashMapException e) {
            isException = true;
            assertEquals(e.getMessage(), "Key " + key + " is not present");
        }
        assertTrue(isException);
    }

    @Test
    public void testPutFailure() {
        String key = "key", value = "value";
        strictHashMap.put(key, value);

        boolean isException = false;
        try {
            strictHashMap.put(key, value);
        } catch (StrictHashMap.StrictHashMapException e) {
            isException = true;
            assertEquals(e.getMessage(), "Key " + key + " is already present");
        }
        assertTrue(isException);

    }

    @Test
    public void testValues() {
        String key = "key", value = "value";
        strictHashMap.put(key, value);
        strictHashMap.put(key + "_1", value);

        List<Object> expected = strictHashMap.getValues();
        assertNotNull(expected);
        assertEquals(expected.size(), 2);
    }


}
