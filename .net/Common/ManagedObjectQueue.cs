namespace AsyncMemManager.Common{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    public class ManagedObjectQueue<T> where T : class, IndexableQueuedObject
    {

        private const int MAX_ARRAY_SIZE = int.MaxValue - 8;
        private const int MAX_POLL_CANDIDATE_CHECK_RANGE = 5;
        
        volatile T[] queue;
        private volatile int size = 0;
        private readonly IComparer<T> comparator;
        
        public ManagedObjectQueue(int initSize, IComparer<T> comparator) {
            this.queue = new T[initSize];
            this.comparator = comparator;
        }
        
        private void Grow(int minCapacity) {
            int oldCapacity = queue.Length;
            // Double size if small; else grow by 50%
            int newCapacity = oldCapacity
                    + ((oldCapacity < 64) ? (oldCapacity + 2) : (oldCapacity >> 1));
            // overflow-conscious code
            if (newCapacity - MAX_ARRAY_SIZE > 0)
                newCapacity = HugeCapacity(minCapacity);
            
            T[] newqueue = new T[newCapacity];
            Array.Copy(queue, newqueue, queue.Length);
            queue = newqueue;
        }
        
        private static int HugeCapacity(int minCapacity) {
            if (minCapacity < 0) // overflow
                throw new OutOfMemoryException();
            return (minCapacity > MAX_ARRAY_SIZE) ? int.MaxValue : MAX_ARRAY_SIZE;
        }
        
        public T GetPollCandidate() {
            // size expected much higher than MAX_POLL_CANDIDATE_CHECK_RANGE, so there should be thread-safe index out of range issue
            for (int i=0; i<MAX_POLL_CANDIDATE_CHECK_RANGE && i < this.size; i++)
            {
                T o = Volatile.Read<T>(ref this.queue[i]);
                if (o != null && o.IsPeekable()) {
                    return o;
                }
            }
            return null;
        }
        
        public bool Add(T e) {
            if (e == null)
                throw new ArgumentNullException();
            int i = size;
            if (i >= queue.Length)
                Grow(i + 1);
            size = i + 1;
            if (i == 0) {
                this.SetQueueItem(0, e);                
            }
            else
                SiftUpUsingComparator(i, e);
            return true;
        }	    

        public T GetAndRemoveAt(int i) {
            // assert i >= 0 && i < size;
            if (i >= size)
            {
                return null;
            }
            
            int s = --size;        

            if (s == i)         
            {// removed last element
                T removed = this.GetAndSetQueueItem(i, null);
                removed.SetIndexInQueue(-1);
                return removed;
            }
            else {        	
                T removed = this.GetQueueItem(i);
                T moved = this.GetAndSetQueueItem(s, null);
                
                SiftDownUsingComparator(i, moved);
                if (this.GetQueueItem(i) == moved) {
                    SiftUpUsingComparator(i, moved);
                }
                
                removed.SetIndexInQueue(-1);
                return removed;
            }
        }
        
        public void SyncPriorityAt(int i) {
            T moved = this.GetQueueItem(i);
            SiftDownUsingComparator(i, moved);
            if (this.GetQueueItem(i) == moved) {
                SiftUpUsingComparator(i, moved);
            }
        }
        
        public T GetAt(int i)
        {
            return this.GetQueueItem(i);
        }
        
        private void SiftUpUsingComparator(int k, T x) {
            while (k > 0) {
                int parent = (k - 1) >> 1;
                T e = this.GetQueueItem(parent);
                if (comparator.Compare(x, e) >= 0)
                    break;
                this.SetQueueItem(k, e);
                k = parent;
            }
            this.SetQueueItem(k, x);
        }    
        
        private void SiftDownUsingComparator(int k, T x) {
            int half = size >> 1;
            while (k < half) {
                int child = (k << 1) + 1;
                T c = this.GetQueueItem(child);
                int right = child + 1;
                if (right < size && comparator.Compare(c, this.GetQueueItem(right)) > 0)
                    c = this.GetQueueItem(child = right);
                if (comparator.Compare(x, c) <= 0)
                    break;
                this.SetQueueItem(k, c);
                k = child;
            }
            this.SetQueueItem(k, x);
        }

        private void SetQueueItem(int idx, T value)
        {
            Volatile.Write<T>(ref this.queue[idx], value);
            value?.SetIndexInQueue(idx);
        }

        private T GetQueueItem(int idx){
            return Volatile.Read<T>(ref this.queue[idx]);
        }
        
        private T GetAndSetQueueItem(int idx, T value)
        {
            T res = Volatile.Read<T>(ref this.queue[idx]);
            this.SetQueueItem(idx, value);
            return res;
        }

        public int Size => this.size;
    }

    public interface IndexableQueuedObject {
        public void SetIndexInQueue(int idx);
        public bool IsPeekable();
    }
}