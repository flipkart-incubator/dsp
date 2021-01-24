package com.flipkart.dsp.utils;

import java.util.*;

/**
 */
public class SortUtills {

    public static List<Long> sortTopological(Map<Long, List<Long>> adjList) {
        List<Long> sortedArray = new LinkedList<>();
        Set<Long> visitedNodes = new HashSet<>();
        Map<Long,Long> inDegree = new HashMap<>();
        List<Long> queue = new LinkedList<>();

        for(Map.Entry<Long, List<Long>> entry: adjList.entrySet()) {
            inDegree.put(entry.getKey(), 0L);
        }

        for(Map.Entry<Long,List<Long>> entry : adjList.entrySet()) {
            for(Long childNode : entry.getValue()) {
                inDegree.put(childNode, inDegree.containsKey(childNode) ? inDegree.get(childNode) + 1 : 1);
            }
        }

        for(Map.Entry<Long, Long> entry : inDegree.entrySet()) {
            if(entry.getValue() == 0) {
                queue.add(entry.getKey());
                visitedNodes.add(entry.getKey());
            }
        }

        while(queue.size() > 0) {
            Long currentNode = queue.get(0);
            queue.remove(0);
            sortedArray.add(currentNode);
            for(Long child : adjList.get(currentNode)) {
                if(!visitedNodes.contains(child)) {
                    inDegree.put(child, inDegree.get(child) - 1);
                    if(inDegree.get(child) == 0) {
                        queue.add(child);
                        visitedNodes.add(child);
                    }
                }
            }
        }

        return sortedArray;
    }
}
