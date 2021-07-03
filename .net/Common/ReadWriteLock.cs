namespace asyncMemManager.Common{
    using System;
    using System.Threading;

    /**
    * key lock for read/write.
    */
    public class ReadWriteLock<T> : IDisposable where T : IReadWriteLockableObject {
        protected volatile bool unlocked = false;
        protected ReadWriteLock<T> updownLock = null;
        private int lockFactor;
        private T lockedObject;

        protected void InitLock(T obj, int lockFactor) {
            this.lockedObject = obj;
            this.lockFactor = lockFactor;
            
            while (true) {
                if (this.IsLockable()) {
                    lock (this.lockedObject.LockerKey) {
                        if (this.IsLockable()) {
                            this.lockedObject.AddLockFactor(this.lockFactor);
                            return;
                        }
                    }
                }

                Thread.Yield();
            }
        }

        private void InitUpdownLock(T obj, int lockFactor) {
            this.lockedObject = obj;
            this.lockFactor = lockFactor;
            this.lockedObject.AddLockFactor(this.lockFactor);
        }
        
        private bool IsLockable() {
            if (this.updownLock != null)
            {
                return this.updownLock.IsLockable();
            }
            
            int objectLockStatus = this.lockedObject.LockStatus;
            return ((this.lockFactor == 2 && (objectLockStatus & 1) == 0)
                    || (this.lockFactor == 1 && objectLockStatus == 0));
        }
        
        private void UnlockWhenSynced() {
            if (updownLock != null) {
                this.updownLock.UnlockWhenSynced();
            }else if (!this.unlocked) {
                this.unlocked = true;
                this.lockedObject.AddLockFactor(-this.lockFactor);
            }		
        }
        
        public void Unlock() {
            if (updownLock != null) {
                this.updownLock.Unlock();
            } else {
                if (!this.unlocked) {
                    lock (this.lockedObject.LockerKey) {
                        this.UnlockWhenSynced();
                    }
                }
            }
        }

        public ReadWriteLock<T> Upgrade() {
            if(this.updownLock != null)
            {
                return this.updownLock.Upgrade();
            }
            
            if (this.lockFactor == 2) {	
                ReadWriteLock<T> replaceLock = new ReadWriteLock<T>();
                bool upgraded = false;
                while (!upgraded) {
                    lock (this.lockedObject.LockerKey) {
                        if ((this.lockedObject.LockStatus & 1) == 0) {
                            replaceLock.InitUpdownLock(this.lockedObject, 1);
                            this.UnlockWhenSynced();
                            upgraded = true;						
                        }else {
                            this.UnlockWhenSynced();
                        }
                    }
                    Thread.Yield();
                }

                while (this.lockedObject.LockStatus > 1) {
                    Thread.Yield();
                }
                
                return this.updownLock = replaceLock;			
            } else {
                return this;
            }
        }

        public ReadWriteLock<T> Downgrade() {
            if(this.updownLock != null)
            {
                return this.updownLock.Downgrade();
            }
            
            if (this.lockFactor == 1) {			
                ReadWriteLock<T> replaceLock = new ReadWriteLock<T>();
                replaceLock.InitUpdownLock(this.lockedObject, 2);
                this.Unlock();
                return this.updownLock = replaceLock;
            } else {
                return this;
            }
        }

        public void Dispose(){
            if (this.updownLock != null) {
                this.updownLock.Dispose();
            } else if (!this.unlocked) {
                this.Unlock();
            }
        }

        /**
        * Read key lock {@link ManagedObjectBase#lockRead()}
        */
        public class ReadLock : ReadWriteLock<T>
        {
            public ReadLock(T obj) {
                this.InitLock(obj, 2);
            }
        }

        /**
        * Read key lock {@link ManagedObjectBase#lockRead()}
        */
        public class WriteLock : ReadWriteLock<T>
        {
            public WriteLock(T obj) {
                this.InitLock(obj, 1);
            }
        }
    }

    public interface IReadWriteLockableObject {
        int LockStatus{
            get;
        }

        void AddLockFactor(int lockfactor);

        object LockerKey
        {
            get;
        }
    }    
}