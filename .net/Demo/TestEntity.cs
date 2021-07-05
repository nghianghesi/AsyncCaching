namespace DemoApp.model
{
    using System;
    using AsyncMemManager.Client.DI;

    public class TestEntity {
        private string stringProperty;
        private int[] largeProperty;
        public const int LARGE_PROPERTY_SIZE = 10000;
        
        public static TestEntity InitLargeObject() {
            TestEntity e = new TestEntity();
            e.stringProperty = string.Format("this is test string {0}", new Random().Next());
            e.largeProperty = new int[LARGE_PROPERTY_SIZE];
            e.largeProperty[0] = new Random().Next();
            return e;
        }
        
        public string GetSomeText() {
            if (string.IsNullOrEmpty(stringProperty) || this.largeProperty[0] == 0)
            {
                throw new InvalidOperationException("Invalid Object State");
            }
            
            return this.stringProperty + " " + this.largeProperty[0];
        }
        
        public class TestEntityAsyncMemSerializer : IAsyncMemSerializer<TestEntity>
        {
            public static readonly TestEntityAsyncMemSerializer Instance = new TestEntityAsyncMemSerializer();
            private TestEntityAsyncMemSerializer()
            {			
                
            }
            
            public string Serialize(TestEntity obj) {
                if (obj.largeProperty == null)
                {
                    return obj.stringProperty;
                }else {
                    return "" + obj.largeProperty[0] + "##" + obj.stringProperty;
                }
            }

            public TestEntity Deserialize(string data) {
                TestEntity e = new TestEntity();
                int indexOfSplitter = data.IndexOf("##");
                if (indexOfSplitter >= 0)
                {
                    e.largeProperty=new int[LARGE_PROPERTY_SIZE];
                    e.largeProperty[0] = int.Parse(data.Substring(0, indexOfSplitter));
                    e.stringProperty = data.Substring(indexOfSplitter+2);
                }else {
                    e.stringProperty = data;
                }
                return e;
            }

            public long EstimateObjectSize(TestEntity obj) {		
                return LARGE_PROPERTY_SIZE + 20;
            }
        }
    }    
}
