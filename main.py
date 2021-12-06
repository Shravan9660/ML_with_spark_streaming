from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


js=SparkContext.getOrCreate()
js.setLogLevel('OFF')
spark=SparkSession(js)
jss = StreamingContext(js,1)


try:
        record = jss.socketTextStream('localhost',6100)
except Exception as e:
        print(e)
        
def readstream(rdd):
        if(len(rdd.collect())>0):
          df=spark.read.json(rdd)
          df.show()
               
record.foreachRDD(lambda x:readstream(x))

jss.start()
jss.awaitTermination()
