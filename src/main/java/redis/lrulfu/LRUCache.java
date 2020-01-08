package redis.lrulfu;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 146. LRU缓存机制
 * 本题采用hash + 双向链表的方式来实现
 * 哈希：查找为O(1)，数据没有顺序，但插入删除慢
 * 双向链表：插入、删除为O(1)，数据有顺序，但查找慢，为O(n)
 */

public class LRUCache {

    private int capacity;
    private Map<Integer, Node> map = new LinkedHashMap<>();
    private Node dummy;
    private Node tail;

    static class Node{
        int key;
        int value;
        Node prev;
        Node next;

        Node(int key, int value){
            this.key = key;
            this.value = value;
        }
    }

    public LRUCache(int capacity) {
        this.capacity = capacity;
        dummy = new Node(0, 0);
        tail = new Node(0, 0);
        dummy.next = tail;
        tail.next = null;
        tail.prev = dummy;
        dummy.prev = null;
    }

    public int get(int key) {
        if (map.containsKey(key)){
            Node node = map.get(key);
            node.prev.next = node.next;
            node.next.prev = node.prev;
            setHead(node);
            return node.value;
        }else{
            return -1;
        }
    }

    public void put(int key, int value) {
        if (map.containsKey(key)){
            Node node = map.get(key);
            node.value = value;
            node.prev.next = node.next;
            node.next.prev = node.prev;
            setHead(node);
        }else{
            Node node = new Node(key, value);
            setHead(node);
            map.put(key, node);
        }

        if (map.size() > capacity){
            map.remove(tail.prev.key);
            tail.prev = tail.prev.prev;
            tail.prev.next = tail;
        }
    }

    public void setHead(Node node){
        node.prev = dummy;
        node.next = dummy.next;
        Node lastHead = dummy.next;
        if (lastHead != null){
            lastHead.prev = node;
        }
        dummy.next = node;
    }

    public static void main(String[] args) {
        LRUCache cache = new LRUCache( 2 /* 缓存容量 */ );

        cache.put(1, 1);
        cache.put(2, 2);
        System.out.println(cache.get(1));
        cache.put(3, 3);    // 该操作会使得密钥 2 作废
        System.out.println(cache.get(2));
        cache.put(4, 4);    // 该操作会使得密钥 1 作废
        System.out.println(cache.get(1));
        System.out.println(cache.get(3));
        System.out.println(cache.get(4));
    }

}

