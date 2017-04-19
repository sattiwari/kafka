package kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created by sinash on 3/27/17.
 */
public class ProducerCallBack implements Callback {
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {

        if(e == null){
            System.out.println(recordMetadata);
        }
        else{
            e.printStackTrace();
        }

    }
}