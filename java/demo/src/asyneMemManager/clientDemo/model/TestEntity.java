package asyneMemManager.clientDemo.model;

import java.util.Random;

import asyncMemManager.common.di.AsyncMemSerializer;

public class TestEntity {
	private String stringProperty;
	private int[] largeProperty;
	private static final int largePropertySize = 10000000;
	
	public static TestEntity initLargeObject() {
		TestEntity e = new TestEntity();
		e.stringProperty = String.format("this is test string %d", new Random().nextInt());
		e.largeProperty = new int[largePropertySize];
		e.largeProperty[0] = new Random().nextInt();
		return e;
	}
	
	public String getSomeText() {
		return this.stringProperty + this.largeProperty[0];
	}
	
	public static class TestEntityAsyncMemSerializer implements AsyncMemSerializer<TestEntity>
	{
		public static final TestEntityAsyncMemSerializer Instance = new TestEntityAsyncMemSerializer();
		private TestEntityAsyncMemSerializer()
		{			
		}
		
		@Override
		public String serialize(TestEntity object) {
			if (object.largeProperty != null)
			{
				return object.stringProperty;
			}else {
				return "" + object.largeProperty + "##" + object.stringProperty;
			}
		}

		@Override
		public TestEntity deserialize(String data) {
			TestEntity e = new TestEntity();
			int indexOfSplitter = data.indexOf("##");
			if (indexOfSplitter>=0)
			{
				e.largeProperty=new int[largePropertySize];
				e.largeProperty[0] = Integer.parseInt(data.substring(0, indexOfSplitter));
				e.stringProperty = data.substring(indexOfSplitter+2);
			}else {
				e.stringProperty = data;
			}
			return e;
		}

		@Override
		public long estimateObjectSize(TestEntity object) {		
			return largePropertySize + 20;
		}
	}
}
