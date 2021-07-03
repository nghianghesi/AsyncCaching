namespace asyncMemManager.common{
    using System;
    using System.Threading;

    /**
    * key lock for read/write.
    */
    public class ReadWriteLock<T> : IDisposable where T : ReadWriteLockableObject {
        protected volatile bool unlocked = false;
        protected ReadWriteLock<T> updownLock = null;
        private int lockFactor;
        private T lockedObject;

        protected void InitLock(T obj, int lockFactor) {
            this.lockedObject = obj;
            this.lockFactor = lockFactor;
            
            while (true) {
                if (this.IsLockable()) {
                    lock (this.lockedObject.GetLockerKey()) {
                        if (this.IsLockable()) {
                            this.lockedObject.AddLockFactor(this.lockFactor);
                            return;
                        }
                    }
                }

                Thread.Yield();
            }
        }

        private void initUpdownLock(T obj, int lockFactor) {
            this.lockedObject = obj;
            this.lockFactor = lockFactor;
            this.lockedObject.AddLockFactor(this.lockFactor);
        }
        
        private bool IsLockable() {
            if (this.updownLock != null)
            {
                return this.updownLock.IsLockable();
            }
            
            int objectLockStatus = this.lockedObject.GetLockFactor();
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
        
        public void unlock() {
            if (updownLock != null) {
                this.updownLock.unlock();
            } else {
                if (!this.unlocked) {
                    lock (this.lockedObject.GetLockerKey()) {
                        this.UnlockWhenSynced();
                    }
                }
            }
        }

        public ReadWriteLock<T> upgrade() {
            if(this.updownLock != null)
            {
                return this.updownLock.upgrade();
            }
            
            if (this.lockFactor == 2) {	
                ReadWriteLock<T> replaceLock = new ReadWriteLock<T>();
                bool upgraded = false;
                while (!upgraded) {
                    lock (this.lockedObject.GetLockerKey()) {
                        if ((this.lockedObject.GetLockFactor() & 1) == 0) {
                            replaceLock.initUpdownLock(this.lockedObject, 1);
                            this.UnlockWhenSynced();
                            upgraded = true;						
                        }else {
                            this.UnlockWhenSynced();
                        }
                    }
                    Thread.Yield();
                }

                while (this.lockedObject.GetLockFactor() > 1) {
                    Thread.Yield();
                }
                
                return this.updownLock = replaceLock;			
            } else {
                return this;
            }
        }

        public ReadWriteLock<T> downgrade() {
            if(this.updownLock != null)
            {
                return this.updownLock.downgrade();
            }
            
            if (this.lockFactor == 1) {			
                ReadWriteLock<T> replaceLock = new ReadWriteLock<T>();
                replaceLock.initUpdownLock(this.lockedObject, 2);
                this.unlock();
                return this.updownLock = replaceLock;
            } else {
                return this;
            }
        }

        public void Dispose(){
            if (this.updownLock != null) {
                this.updownLock.Dispose();
            } else if (!this.unlocked) {
                this.unlock();
            }
        }

        /**
        * Read key lock {@link ManagedObjectBase#lockRead()}
        */
        private class ReadLock : ReadWriteLock<T>
        {
            public ReadLock(T obj) {
                this.InitLock(obj, 2);
            }
        }

        /**
        * Read key lock {@link ManagedObjectBase#lockRead()}
        */
        private class WriteLock : ReadWriteLock<T>
        {
            public WriteLock(T obj) {
                this.InitLock(obj, 1);
            }
        }
    }

    public interface ReadWriteLockableObject {
        int GetLockFactor();

        void AddLockFactor(int lockfactor);

        object GetLockerKey();
    }    
}