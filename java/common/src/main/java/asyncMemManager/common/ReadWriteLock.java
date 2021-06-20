package asyncMemManager.common;

/**
 * key lock for read/write.
 */
public class ReadWriteLock<T extends ReadWriteLock.ReadWriteLockableObject> implements AutoCloseable {
	protected volatile boolean unlocked = false;
	protected ReadWriteLock<T> updownLock = null;
	private int lockFactor;
	private T object;

	protected ReadWriteLock(T obj, int lockFactor) {
		this.object = obj;
		this.lockFactor = lockFactor;

		while (true) {
			if (this.lockable()) {
				synchronized (this.object.getKeyLocker()) {
					if (this.lockable()) {
						this.object.addLockFactor(this.lockFactor);
						return;
					}
				}
			}

			Thread.yield();
		}
	}

	private boolean lockable() {
		if (this.updownLock == null)
		{
			return ((lockFactor < 0 && this.object.getLockFactor() <= 0)
					|| (lockFactor > 0 && this.object.getLockFactor() == 0));
		}else {
			return this.updownLock.lockable();
		}
	}

	public void unlock() {
		if (updownLock != null) {
			this.updownLock.unlock();
		} else {
			if (!this.unlocked) {
				synchronized (this.object.getKeyLocker()) {
					if (!this.unlocked) {
						this.unlocked = true;
						this.object.addLockFactor(-this.lockFactor);
					}
				}
			}
		}
	}

	public ReadWriteLock<T> upgrade() {
		if(this.updownLock != null)
		{
			return this.updownLock.upgrade();
		}
		
		if (this.lockFactor < 0) {
			this.unlock();
			return this.updownLock = new ReadWriteLock<T>(this.object, Math.abs(this.lockFactor));
		} else {
			return this;
		}
	}

	public ReadWriteLock<T> downgrade() {
		if(this.updownLock != null)
		{
			return this.updownLock.downgrade();
		}
		
		if (this.lockFactor > 0) {
			this.unlock();
			return this.updownLock = new ReadWriteLock<T>(this.object, -this.lockFactor);
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

		Object getKeyLocker();
	}

	/**
	 * Read key lock {@link ManagedObjectBase#lockRead()}
	 */
	public static class ReadLock<T extends ReadWriteLock.ReadWriteLockableObject> extends ReadWriteLock<T> {
		public ReadLock(T obj) {
			super(obj, -1);
		}
	}

	/**
	 * Read key lock {@link ManagedObjectBase#lockRead()}
	 */
	public static class WriteLock<T extends ReadWriteLock.ReadWriteLockableObject> extends ReadWriteLock<T> {
		public WriteLock(T obj) {
			super(obj, 1);
		}
	}
}