package asyncMemManager.common;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReferenceArray;

import asyncMemManager.common.di.IndexableQueuedObject;

public class ManagedObjectQueue<T extends IndexableQueuedObject> {

	private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
	private static final int MAX_POLL_CANDIDATE_CHECK_RANGE = 5;
	
	volatile AtomicReferenceArray<T> queue;
    private volatile int size = 0;
    private final Comparator<T> comparator;
    
    public ManagedObjectQueue(int initSize, Comparator<T> comparator) {
        this.queue = new AtomicReferenceArray<>(initSize);
        this.comparator = comparator;
    }
    
    private void grow(int minCapacity) {
        int oldCapacity = queue.length();
        // Double size if small; else grow by 50%
        int newCapacity = oldCapacity
                + ((oldCapacity < 64) ? (oldCapacity + 2) : (oldCapacity >> 1));
        // overflow-conscious code
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        
        AtomicReferenceArray<T> newqueue = new AtomicReferenceArray<>(newCapacity);
        for(int i=0; i<queue.length(); i++) {
        	newqueue.set(i, queue.get(i));
        }
        queue = newqueue;
    }
    
    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
    }
    
	public T getPollCandidate() {
    	// size expected much higher than MAX_POLL_CANDIDATE_CHECK_RANGE, so there should be thread-safe index out of range issue
        for (int i=0; i<MAX_POLL_CANDIDATE_CHECK_RANGE && i < this.size; i++)
        {
        	T o = this.queue.get(i);
        	if (o != null && o.isPeekable()) {
        		return o;
        	}
        }
        return null;
    }
    
    public boolean add(T e) {
        if (e == null)
            throw new NullPointerException();
        int i = size;
        if (i >= queue.length())
            grow(i + 1);
        size = i + 1;
        if (i == 0) {
            queue.set(0, e);
            e.setIndexInQueue(0);
        }
        else
        	siftUpUsingComparator(i, e);
        return true;
    }	    

	public T removeAt(int i) {
        // assert i >= 0 && i < size;
		if (i >= size)
		{
			return null;
		}
		
        int s = --size;        

        if (s == i)         
        {// removed last element
        	T removed = queue.getAndSet(i, null);
        	removed.setIndexInQueue(-1);
        	return removed;
        }
        else {        	
        	T removed = queue.get(i);
        	T moved = queue.getAndSet(s, null);
        	
            siftDownUsingComparator(i, moved);
            if (queue.get(i) == moved) {
            	siftUpUsingComparator(i, moved);
            }
            
            removed.setIndexInQueue(-1);
        	return removed;
        }
    }
	
	public void syncPriorityAt(int i) {
		T moved = queue.get(i);
        siftDownUsingComparator(i, moved);
        if (queue.get(i) == moved) {
        	siftUpUsingComparator(i, moved);
        }
	}
	
	public T getAt(int i)
	{
		return this.queue.get(i);
	}
    
	private void siftUpUsingComparator(int k, T x) {
        while (k > 0) {
            int parent = (k - 1) >>> 1;
            T e = queue.get(parent);
            if (comparator.compare(x, e) >= 0)
                break;
            queue.set(k, e);
            e.setIndexInQueue(k);
            k = parent;
        }
        queue.set(k, x);
        x.setIndexInQueue(k);
    }    
    
	private void siftDownUsingComparator(int k, T x) {
        int half = size >>> 1;
        while (k < half) {
            int child = (k << 1) + 1;
            T c = queue.get(child);
            int right = child + 1;
            if (right < size && comparator.compare(c, queue.get(right)) > 0)
                c = queue.get(child = right);
            if (comparator.compare(x, c) <= 0)
                break;
            queue.set(k, c);
            c.setIndexInQueue(k);
            k = child;
        }
        queue.set(k, x);
        x.setIndexInQueue(k);
    }
    
    public int size() 
    {
    	return this.size;
    }
}
