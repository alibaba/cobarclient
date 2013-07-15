package com.alibaba.cobar.client.merger;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.testng.annotations.Test;

@Test
public class ConcurrentSortMergerTest {
    
    public void testMerge() throws Exception{
        ConcurrentSortMerger<Integer> merger = new ConcurrentSortMerger<Integer>();
        merger.setComparator(new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
                return o1.intValue() - o2.intValue();
            }
        });
        merger.afterPropertiesSet();
        List<List<Integer>> entities = new ArrayList<List<Integer>>();
        entities.add(Arrays.asList(1, 2, 4));
        entities.add(Arrays.asList(3, 5, 8, 10));
        List<Integer> result = merger.merge(entities);
        assertEquals(7, result.size());
        assertTrue(Arrays.equals(new Integer[]{1,2,3,4,5,8,10}, result.toArray(new Integer[result.size()])));
        merger.destroy();
    }
}
