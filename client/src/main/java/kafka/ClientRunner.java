package kafka;

/**
 * Created by stiwari on 4/18/2017 AD.
 */
public class ClientRunner {

    public static void main(String[] args) {
        SampleProducer producer = new SampleProducer();
        while (true) {
            producer.fireAndForget("test-sample", "key", "value");
        }
    }
}
