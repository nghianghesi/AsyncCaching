namespace AsyncMemManager.Client
{
    using DI;
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;

    public class AsyncMemManager : IAsyncMemManager
    {        
        // this is for special marker only.
        private static readonly ManagedObjectQueue<ManagedObjectBase> queuedForManageCandle = new ManagedObjectQueue<ManagedObjectBase>(0, null);
        private static readonly ManagedObjectQueue<ManagedObjectBase> obsoletedManageCandle = new ManagedObjectQueue<ManagedObjectBase>(0, null);


        public ISetupObject<T> Manage<T>(string flowKey, T obj, IAsyncMemSerializer<T> serializer) 
        {
            return null;
        }  
        
        public string DebugInfo()
        {
            return string.Empty;
        }        

        public void Dispose(){
            
        }   

        protected class AsyncMemManagerContainObject
        {
            protected readonly AsyncMemManager manager;
            protected AsyncMemManagerContainObject(AsyncMemManager m)
            {
                this.manager = m;
            }
        }
	
        public class SetupObject<T> : AsyncMemManagerContainObject, ISetupObject<T>
        {            
            protected SetupObject(AsyncMemManager manager) : base (manager){
            }

            public IAsyncObject<T> AsyncO()
            {
                return null;
            }

            public T O()
            {
                return default(T);
            }

            public void Dispose(){
                
            }
        }
        
        public class AsyncObject<T> : AsyncMemManagerContainObject, IAsyncObject<T>
        {		           
            protected AsyncObject(AsyncMemManager manager) : base (manager){
            }

            public R Supply<R>(Func<T,R> f)
            {
                return default(R);
            }
            public void Apply(Action<T> f)
            {
                return ;
            }

            public void Dispose(){

            }
        }
        
        abstract class ManagedObjectBase : IndexableQueuedObject, ReadWriteLockableObject
        {
            /***
            * key value to lookup object, this is auto unique generated
            * also used as key for synchronize access vs management
            */
            readonly Guid key;
            
            /**
            * flow key, this is used for estimate waiting time
            */
            readonly string flowKey;
            
            /**
            * original object
            */
            volatile object obj;
            
            /**
            * time object managed
            */
            volatile DateTime startTime;
            
            /**
            * time object expected to be retrieved for async, this is average from previous by keyflow
            */
            volatile DateTime hotTime;
            
            /**
            * estimated by serializer, size of object
            */
            readonly long estimatedSize;
            
            /**
            * the candle contain this object, used for fast cleanup, removal
            */
            private volatile ManagedObjectQueue<ManagedObjectBase> containerCandle;
            
            /**
            * the index of object in candle, used for fast removal
            */
            volatile int indexInCandle = -1;
            
            volatile int numberOfAccess = 0;
            
            /**
            * the serializer to ser/des object for persistence.
            */
            readonly SerializerGeneral serializer;

            /**
            * init  ManagedObject 
            */
            public ManagedObjectBase(string flowKey, long estimatedSize, SerializerGeneral serializer) {
                this.flowKey = flowKey;
                this.key = Guid.NewGuid();
                this.startTime = this.hotTime = LocalDateTime.now();
                this.estimatedSize = estimatedSize;
                this.serializer = serializer;
            }

            /**
            * if object still being setup. object start to be managed setup closed
            */
            volatile bool doneSetup = false;
            
            /**
            * counting of async flows, object stop to be managed when all aync closed
            */
            int asyncCounter = 0;
            
            bool isObsoleted() {
                return this.doneSetup && Interlocked.Read(ref this.asyncCounter) == 0;
            }
            
            /**
            * get management state to have associated action.
            * this is for roughly estimate, as not ensured thread-safe.
            */
            ManagementState getManagementState()
            {
                lock (this) {
                    ManagedObjectQueue<ManagedObjectBase> c = this.containerCandle;  
                    if (c == null)
                    {
                        return ManagementState.None;
                    }else if (c == AsyncMemManager.queuedForManageCandle){
                        return ManagementState.Queued;
                    }else if (c == AsyncMemManager.obsoletedManageCandle){
                        return ManagementState.Obsoleted;
                    }else {
                        return ManagementState.Managing;
                    }
                }
            }
            
            /**
            * return previous containerCandel
            */
            ManagedObjectQueue<ManagedObjectBase> setManagementState(ManagedObjectQueue<ManagedObjectBase> containerCandle)
            {
                lock (this) {
                    ManagedObjectQueue<ManagedObjectBase> prev = this.containerCandle;
                    this.containerCandle = containerCandle;
                    return prev;				
                }
            }
        
            /**
            * used for read/write locking this managed object. 
            */
            private volatile int readWriteCounter = 0;
            
            public void setIndexInQueue(int idx)
            {
                this.indexInCandle = idx;
            }
            
            /**
            * whether this object is available for cleanup
            */
            public bool isPeekable() {
                return this.readWriteCounter == 0 && this.getManagementState() == ManagementState.Managing && this.indexInCandle >= 0;
            }
            
            /**
            * read locking, used for async flows access object, to ensure data not interfered
            */
            ReadWriteLock<ManagedObjectBase> lockRead()
            {
                return new ReadWriteLock.ReadLock<ManagedObjectBase>(this);
            }
            
            /**
            * manage locking, used for cleanup, remove process, to ensure data not interfered 
            */
            ReadWriteLock<ManagedObjectBase> lockManage()
            {
                return new ReadWriteLock.WriteLock<ManagedObjectBase>(this);
            }
            
            public int getLockFactor() {
                return this.readWriteCounter;
            }
            
            public void addLockFactor(int lockfactor) {
                this.readWriteCounter += lockfactor;
            }
            
            public Object getLockerKey() {
                return this.key;
            }
        }
        
        static enum ManagementState
        {
            None,
            Queued,
            Managing,
            Obsoleted
        }
	
	    /**
	        * Generic class for ManagedObject
        */
        class ManagedObject<T> : ManagedObjectBase
        {
            volatile ManagedObject(string flowKey, T obj, long estimatedSize, SerializerGeneral serializer)
            {
                super(flowKey, estimatedSize, serializer);
                this.obj = obj;
            }
        }        

        private class SerializerGeneral
        {	
            private static IDictionary<Type, SerializerGeneral> instances = new ConcurrentDictionary<Type, SerializerGeneral>();
            
            private Func<object, string> serialzeFunc;
            private Func<string, object> deserializeFunc;
            private Func<object, long> estimateObjectSizeFunc;
            
            // it's ok to in-thread safe here, as object override wouldn't cause any issue.
            public static SerializerGeneral GetSerializerBaseInstance<T>(IAsyncMemSerializer<T> serializer)
            {
                
                if(SerializerGeneral.instances.TryGetValue(serializer.GetType(), out SerializerGeneral inst) 
                    && inst == null)
                {
                    inst = new SerializerGeneral();

                    inst.serialzeFunc = (obj) => {
                        return serializer.Serialize((T)obj);
                    };		
                    
                    inst.deserializeFunc = (data) => {
                        return serializer.Deserialize(data);
                    };
                    
                    inst.estimateObjectSizeFunc = (obj) => {
                        return serializer.EstimateObjectSize((T)obj);
                    };

                    SerializerGeneral.instances.Add(serializer.GetType(), inst);
                }
                
                return SerializerGeneral.instances[serializer.GetType()];
            }

            private SerializerGeneral()
            {}
                       
            public string Serialize(object obj)
            {
                return this.serialzeFunc(obj);
            }
            
            public T deserialize<T>(string data)
            {
                return (T)this.deserializeFunc(data);
            }
            
            public long estimateObjectSize(object obj)
            {
                return this.estimateObjectSizeFunc(obj);
            }
        }        
    }
}
