package asyncCache.common;

public class SingleReadMemcache implements asyncCache.common.di.SingleReadMemcache {

	public SingleReadMemcache(asyncCache.common.di.ColdTimeCalculator calculator, 
			asyncCache.common.di.Persistence persistence) 
	{
		
	}
	
	@Override
	public String put(String flowKey, byte[] data) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte retrieve(String key) {
		// TODO Auto-generated method stub
		return 0;
	}

}
