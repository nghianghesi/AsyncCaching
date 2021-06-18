package asyncCache.client;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import asyncMemManager.common.di.Persistence;

public class MemCacheServerPersistence implements Persistence{

	@Override
	public CompletableFuture<UUID> store(UUID key, byte[] data, long expectedDuration) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] retrieve(UUID key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove(UUID key) {
		// TODO Auto-generated method stub
		
	}

}
