import com.rabbitmq.client.*;

import java.io.IOException;

public class RecipientListReceiver {

    private final static String QUEUE_NAME = "banks_queue";
    //when you start this receiver, the messages hanging will be received if any :)

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages from GetBanks. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String loanWithBanks = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + loanWithBanks + "' from GetBanks");

                RecipientListWorker rw = new RecipientListWorker();
                try {
                    rw.processMessage(loanWithBanks);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        };
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }

}
