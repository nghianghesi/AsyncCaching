package asyncMemManager.common;

import java.util.Arrays;
import java.util.Comparator;

import asyncMemManager.common.di.IndexableQueuedObject;

public class ManagedObjectQueue<T extends IndexableQueuedObject> {

	private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
	private static final int MAX_POLL_CANDIDATE_CHECK_RANGE = 5;
	
	Object[] queue;
    private int size = 0;
    private final Comparator<T> comparator;
    
    public ManagedObjectQueue(int initSize, Comparator<T> comparator) {
        this.queue = new Object[initSize];
        this.comparator = comparator;
    }
    
    private void grow(int minCapacity) {
        int oldCapacity = queue.length;
        // Double size if small; else grow by 50%
        int newCapacity = oldCapacity
                + ((oldCapacity < 64) ? (oldCapacity + 2) : (oldCapacity >> 1));
        // overflow-conscious code
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        queue = Arrays.copyOf(queue, newCapacity);
    }
    
    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
    }
    
    @SuppressWarnings("unchecked")
	public T getPollCandidate() {
    	// size expected much higher than MAX_POLL_CANDIDATE_CHECK_RANGE, so there should be thread-safe index out of range issue
        for (int i=0; i<MAX_POLL_CANDIDATE_CHECK_RANGE && i<this.size; i++)
        {
        	if (((T)this.queue[i]).isPeekable()) {
        		return (T)this.queue[i];
        	}
        }
        return null;
    }
    
    public boolean add(T e) {
        if (e == null)
            throw new NullPointerException();
        int i = size;
        if (i >= queue.length)
            grow(i + 1);
        size = i + 1;
        if (i == 0)
            queue[0] = e;
        else
        	siftUpUsingComparator(i, e);
        return true;
    }	    

	@SuppressWarnings("unchecked")
	public T removeAt(int i) {
        // assert i >= 0 && i < size;
        int s = --size;
        if (s == i) 
        {// removed last element
        	((T)queue[i]).setIndexInQueue(-1);
            queue[i] = null;
        }
        else {
        	T moved = (T)queue[s];
            queue[s] = null;
            siftDownUsingComparator(i, moved);
            if (queue[i] == moved) {
            	siftUpUsingComparator(i, moved);
                if (queue[i] != moved) 
                {
                	 moved.setIndexInQueue(-1);
                    return moved;
                }
            }
        }
        
        return null;
    }
	
	@SuppressWarnings("unchecked")
	public void syncPriorityAt(int i) {
		T moved = (T)queue[i];
        siftDownUsingComparator(i, moved);
        if (queue[i] == moved) {
        	siftUpUsingComparator(i, moved);
        }
	}
    
    @SuppressWarnings("unchecked")
	private void siftUpUsingComparator(int k, T x) {
        while (k > 0) {
            int parent = (k - 1) >>> 1;
            T e = (T)queue[parent];
            if (comparator.compare(x, e) >= 0)
                break;
            queue[k] = e;
            e.setIndexInQueue(k);
            k = parent;
        }
        queue[k] = x;
        x.setIndexInQueue(k);
    }    
    
    @SuppressWarnings("unchecked")
	private void siftDownUsingComparator(int k, T x) {
        int half = size >>> 1;
        while (k < half) {
            int child = (k << 1) + 1;
            T c = (T)queue[child];
            int right = child + 1;
            if (right < size && comparator.compare(c, (T)queue[right]) > 0)
                c = (T)queue[child = right];
            if (comparator.compare(x, c) <= 0)
                break;
            queue[k] = c;
            c.setIndexInQueue(k);
            k = child;
        }
        queue[k] = x;
        x.setIndexInQueue(k);
    }
    
    public int size() 
    {
    	return this.size;
    }
}
