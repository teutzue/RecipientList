
import com.rabbitmq.client.*;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.json.Json;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RecipientListWorker {

    //get the json and send messages using routing

    private static final String EXCHANGE_NAME = "recipientList_translator";

    public void processMessage(String incomingMessage) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //direct exchange
        // a message goes to the queues whose binding key exactly matches the routing key of the message
        // in this case the binding key will be the name of the bank
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        //you need to send smth like: (ssn, credit score, loan amount, loan duration)
        //1. get the banks
        //2.


        JSONObject loan_json = new JSONObject(incomingMessage);
        String ssn = loan_json.getString("ssn");
        Double loanAmount = loan_json.getDouble("loanAmount");
        String loanDuration = loan_json.getString("loanDuration");
        int creditScore = loan_json.getInt("creditScore");
        JSONArray banks = loan_json.getJSONArray("banks");
        System.out.println("SSN extracted: " + ssn + "; Loan amount: " + loanAmount+ "; Loan duration "
                + loanDuration + "; Credit Score: " + creditScore + "; Banks: " + banks.toString());

        JsonObject message = Json.createObjectBuilder()
                .add("ssn", ssn)
                .add("creditScore", creditScore)
                .add("loanAmount", loanAmount)
                .add("loanDuration", loanDuration)
                .build();

        System.out.println("The message to be sent to the banks is: " + message);



        if (banks != null) {
            int len = banks.length();
            for (int i=0;i<len;i++){
                //create your header with the information needed and attach to basic publish
                Map<String,Object> headerMap = new HashMap();
                headerMap.put("bank", banks.get(i).toString());

                AMQP.BasicProperties props = new AMQP.BasicProperties
                        .Builder()
                        .headers(headerMap)
                        .build();

                channel.basicPublish(EXCHANGE_NAME, banks.get(i).toString(), props, message.toString().getBytes("UTF-8"));
                System.out.println("Header sent: " + props);
                System.out.println(banks.get(i).toString());
                System.out.println(" [x] Sent to'" + banks.get(i).toString() + "':'" + message + "' From RecipientListWorker");
            }
        }

        channel.close();
        connection.close();
    }
}
