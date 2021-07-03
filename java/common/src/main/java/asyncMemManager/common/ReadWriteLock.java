package asyncMemManager.common;

/**
 * key lock for read/write.
 */
public class ReadWriteLock<T extends ReadWriteLock.ReadWriteLockableObject> implements AutoCloseable {
	protected volatile boolean unlocked = false;
	protected ReadWriteLock<T> updownLock = null;
	private int lockFactor;
	private T object;

	protected void initLock(T obj, int lockFactor) {
		this.object = obj;
		this.lockFactor = lockFactor;
		
		while (true) {
			if (this.lockable()) {
				synchronized (this.object.getLockerKey()) {
					if (this.lockable()) {
						this.object.addLockFactor(this.lockFactor);
						return;
					}
				}
			}

			Thread.yield();
		}
	}

	private void initUpdownLock(T obj, int lockFactor) {
		this.object = obj;
		this.lockFactor = lockFactor;
		this.object.addLockFactor(this.lockFactor);
	}
	
	private boolean lockable() {
		if (this.updownLock != null)
		{
			return this.updownLock.lockable();
		}
		
		int lockStatus = this.object.getLockFactor();
		return ((lockFactor == 2 && (lockStatus & 1) == 0)
				|| (lockFactor == 1 && lockStatus == 0));
	}
	
	private void unlockWhenSynced() {
		if (updownLock != null) {
			this.updownLock.unlockWhenSynced();
		}else if (!this.unlocked) {
			this.unlocked = true;
			this.object.addLockFactor(-this.lockFactor);
		}		
	}
	
	public void unlock() {
		if (updownLock != null) {
			this.updownLock.unlock();
		} else {
			if (!this.unlocked) {
				synchronized (this.object.getLockerKey()) {
					this.unlockWhenSynced();
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
			ReadWriteLock<T> replaceLock = new ReadWriteLock<>();
			boolean upgraded = false;
			while (!upgraded) {
				synchronized (this.object.getLockerKey()) {
					if ((this.object.getLockFactor() & 1) == 0) {
						replaceLock.initUpdownLock(this.object, 1);
						this.unlockWhenSynced();
						upgraded = true;						
					}else {
						this.unlockWhenSynced();
					}
				}
				Thread.yield();
			}

			while (this.object.getLockFactor() > 1) {
				Thread.yield();
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
			ReadWriteLock<T> replaceLock = new ReadWriteLock<>();
			replaceLock.initUpdownLock(this.object, 2);
			this.unlock();
			return this.updownLock = replaceLock;
		} else {
			return this;
		}
	}

	@Override
	public void close() throws Exception {
		if (this.updownLock != null) {
			this.updownLock.close();
		} else if (!this.unlocked) {
			this.unlock();
		}
	}

	public static interface ReadWriteLockableObject {
		int getLockFactor();

		void addLockFactor(int lockfactor);

		Object getLockerKey();
	}

	/**
	 * Read key lock {@link ManagedObjectBase#lockRead()}
	 */
	public static class ReadLock<T extends ReadWriteLock.ReadWriteLockableObject> extends ReadWriteLock<T> {
		public ReadLock(T obj) {
			this.initLock(obj, 2);
		}
	}

	/**
	 * Read key lock {@link ManagedObjectBase#lockRead()}
	 */
	public static class WriteLock<T extends ReadWriteLock.ReadWriteLockableObject> extends ReadWriteLock<T> {
		public WriteLock(T obj) {
			this.initLock(obj, 1);
		}
	}
}