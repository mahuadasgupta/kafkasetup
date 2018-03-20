 //###################################St344t   Kafka Producer Sample.#################################################

//Before creating the application, first start ZooKeeper and Kafka broker then create 
//your own topic in Kafka broker using create topic command. 
// example:kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1  --partitions 1 --topic st344tTopic
//After that create a java file  named st344tProducer.java and type in the following coding. Once  the coding is done . compile and execute the code.
//Compile:javac -cp "/opt/app/kafka_2.12-0.0/lib/*" St344tProducer.java
//Execute on a terminal:java -cp "/opt/app/kafka_2.12-0.0/lib/*":. St344tProducer st344tTopic

//import util.properties packages
import java.util.Properties;
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

//Create java class named "SimpleProducer"
public class St344tProducerRepl {
   
   public static void main(String[] args) throws Exception{
      
      // Check arguments length value
      if(args.length == 0){
         System.out.println("Enter topic name");
         return;
      }
      
      //Assign topicName to string variable
      String topicName = args[0].toString();
      
      // create instance for properties to access producer configs   
      Properties props = new Properties();
      
      //Assign localhost id
      props.put("bootstrap.servers", "localhost:9093,localhost:9094,localhost:9095");
      
      //Set acknowledgements for producer requests.      
      props.put("acks", "all");
      
      //If the request fa:ils, the producer can automatically retry,
      props.put("retries", 0);
      
      //Specify buffer size in config
      props.put("batch.size", 16384);
 
      
      //The buffer.memory controls the total amount of memory available to the producer for buffering.   
      props.put("buffer.memory", 33554432);
      
      props.put("key.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");
         
      props.put("value.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");
      
      Producer<String, String> producer = new KafkaProducer
         <String, String>(props);
            
      for(int i = 0; i < 20; i++)
         producer.send(new ProducerRecord<String, String>(topicName, 
            Integer.toString(i), Integer.toString(i)));
               System.out.println("Message sent successfully");
               producer.close();
   }
}



