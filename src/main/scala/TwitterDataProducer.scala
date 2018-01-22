import java.util._
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import org.apache.kafka.clients.producer._
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import scala.collection.JavaConverters._

object TwitterDataProducer {
  var props: Properties = _
  var auth: OAuth1 = _
  var producer: KafkaProducer[String,String] = _
  var queue: BlockingQueue[String] = _
  var endPoint: StatusesFilterEndpoint = _
  var client: Client = _

  var topic = "test-1"
  var consumerKey = "xxxx"
  var consumerKeySecret = "xxxx"
  var accessToken = "xxxx"
  var accessSecret = "xxxx"

  def setProps() = {
    props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  }

  def setParams() = {
    queue = new LinkedBlockingQueue[String](1000)
    endPoint = new StatusesFilterEndpoint()
    endPoint.filterLevel(Constants.FilterLevel.Low)
    endPoint.languages(scala.List("en").asJava)
    endPoint.trackTerms(scala.List("#india").asJava)
    auth = new OAuth1(consumerKey,consumerKeySecret,accessToken,accessSecret)
    client = new ClientBuilder().hosts(Constants.STREAM_HOST).endpoint(endPoint).authentication(auth).processor(new StringDelimitedProcessor(queue)).build()
    producer = new KafkaProducer[String,String](props)
  }
  def cleanUp() = {
    client.stop()
    producer.close()
  }
  def main(args: Array[String]) = {
    setProps()
    setParams()
    client.connect()
    for(i <- 1 to 10) {
      var msg = new ProducerRecord(topic,""+i,queue.take())
      producer.send(msg)
    }
    cleanUp()
  }
}
