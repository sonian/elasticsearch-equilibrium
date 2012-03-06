package com.sonian.elasticsearch;

import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * @author dakrone
 */
public class DiskShardsAllocatorTests {

    @Test
    public void testEnoughDiskForShard() {
        assertThat(2 + 2, equalTo(4));
    }
}
