package kafka.producer;

/**
 * Created by stiwari on 4/18/2017 AD.
 */
public class Customer {

    private int customerID;
    private String customerName;

    public Customer(int ID, String name) {
        this.customerID = ID;
        this.customerName = name;
    }

    public int getID() {
        return customerID;
    }

    public String getName() {
        return customerName;
    }

}
