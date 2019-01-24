package com.hsrw.myapp;

import android.os.Bundle;
import android.os.Handler;
import android.os.Message;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;


public class MainActivity extends AppCompatActivity {
    private static final String EXCHANGE_KEY_RED = "Library";
    private static final String EXCHANGE_KEY_BLUE = "Mensa";
    private static final String EXCHANGE_KEY_FAB = "Fablab";

    private static final String EXCHANGE_NAME = "Publisher";

    private String currentPublishKey = EXCHANGE_KEY_BLUE;
    private String currentSubscribeKey = EXCHANGE_KEY_BLUE;

    //private String currentPublishKey2 = EXCHANGE_KEY_BLUE;
    //private String currentSubscribeKey2 = EXCHANGE_KEY_BLUE;
    //private String currentPublishKey3 = EXCHANGE_KEY_FAB;
    //private String currentSubscribeKey3 = EXCHANGE_KEY_FAB;


    Connection connection;
    private Handler incomingMessageHandler;
    ConnectionFactory factory = new ConnectionFactory();
    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        setupConnectionFactory();
        publishToAMQP();
        setupPubBlueButton();
        setupPubRedButton();
        setupPubFabButton();
        setupSubBlueButton();
        setupSubRedButton();
        setupSubFabButton();

        incomingMessageHandler = new Handler() {
            @Override
            public void handleMessage(Message msg) {
                String message = msg.getData().getString("msg");
                TextView tv = (TextView) findViewById(R.id.textView);
                Date now = new Date();
                SimpleDateFormat ft = new SimpleDateFormat ("hh:mm:ss");
                // update the text view with the current subscriber key (can be "blue" or "red") + the current time + the message received
                tv.append(currentSubscribeKey  + ' ' + ft.format(now) + ' ' + message + '\n');
            }
        };
        subscribe(incomingMessageHandler);
    }



    void setupPubBlueButton() {
        Button button = (Button) findViewById(R.id.publishBlue);
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View arg0) {
                // update the publish routing key to "blue"
                currentPublishKey = EXCHANGE_KEY_BLUE;
                EditText et = (EditText) findViewById(R.id.text);
                publishMessage(et.getText().toString());
                et.setText("");
            }
        });
    }

    void setupPubRedButton() {
        Button button = (Button) findViewById(R.id.publishRed);
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View arg0) {
                // update the publish routing key to "red"
                currentPublishKey = EXCHANGE_KEY_RED;
                EditText et = (EditText) findViewById(R.id.text);
                publishMessage(et.getText().toString());
                et.setText("");
            }
        });
    }

    void setupPubFabButton() {
        Button button = (Button) findViewById(R.id.publishFab);
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View arg0) {
                // update the publish routing key to "red"
                currentPublishKey = EXCHANGE_KEY_FAB;
                EditText et = (EditText) findViewById(R.id.text);
                publishMessage(et.getText().toString());
                et.setText("");
            }
        });
    }

    void setupSubBlueButton() {
        Button button = (Button) findViewById(R.id.subscribeBlue);
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View arg0) {
                // update the subscribe routing key to "blue"
                currentSubscribeKey = EXCHANGE_KEY_BLUE;
                subscribe(incomingMessageHandler);
            }
        });
    }

    void setupSubRedButton() {
        Button button = (Button) findViewById(R.id.subscribeRed);
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View arg0) {
                // update the subscribe routing key to "red"
                currentSubscribeKey = EXCHANGE_KEY_RED;
                subscribe(incomingMessageHandler);
            }
        });
    }


    void setupSubFabButton() {
        Button button = (Button) findViewById(R.id.subscribeFab);
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View arg0) {
                // update the subscribe routing key to "red"
                currentSubscribeKey = EXCHANGE_KEY_RED;
                subscribe(incomingMessageHandler);
            }
        });
    }

    Thread subscribeThread;
    Thread publishThread;
    @Override
    protected void onDestroy() {
        super.onDestroy();
        publishThread.interrupt();
        subscribeThread.interrupt();
    }

    private BlockingDeque<String> queue = new LinkedBlockingDeque<String>();
    void publishMessage(String message) {
        //Adds a message to internal blocking queue
        try {
            Log.d("","[q] " + message);
            queue.putLast(message);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private void setupConnectionFactory() {
        factory.setAutomaticRecoveryEnabled(false);
        factory.setUsername("admin");
        factory.setPassword("admin");
        // insert the IP of the machine where the server is running ( you can check the IP via the command line utilities IPCONFIG or IFCONFIG)
        factory.setHost("192.168.0.143");
        factory.setPort(5672);

    }

    void subscribe(final Handler handler)
    {
        if (subscribeThread!= null)
            subscribeThread.interrupt();
        subscribeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        // Create a new broker connection
                        Connection connection = factory.newConnection();
                        Channel channel = connection.createChannel();
                        channel.basicQos(1);
                        // declare an exchange named "color", type "direct" in order to allow routing message with a routingKey ("blue" or "red")
                        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
                        // declare a server-named exclusive, autodelete, non-durable queue.
                        String queueName = channel.queueDeclare().getQueue();
                        // Bind a queue to the exchange "color" for a specific routingKey (currentSubscribeKey can be "blue" or "red")
                        channel.queueBind(queueName, EXCHANGE_NAME, currentSubscribeKey);
                        QueueingConsumer consumer = new QueueingConsumer(channel);
                        channel.basicConsume(queueName, true, consumer);

                        // Process deliveries
                        while (true) {
                            QueueingConsumer.Delivery delivery = consumer.nextDelivery();

                            String message = new String(delivery.getBody());
                            Log.d("","[r] " + message);

                            Message msg = handler.obtainMessage();
                            Bundle bundle = new Bundle();

                            bundle.putString("msg", message);
                            msg.setData(bundle);
                            handler.sendMessage(msg);
                        }
                    } catch (InterruptedException e) {
                        break;
                    } catch (Exception e1) {
                        Log.d("", "Connection broken: " + e1.getClass().getName());
                        try {
                            Thread.sleep(4000); //sleep and then try again
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            }
        });
        subscribeThread.start();
    }

    public void publishToAMQP()
    {
        publishThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        connection = factory.newConnection();
                        Channel ch = connection.createChannel();  //debug here
                        ch.confirmSelect();

                        while (true) {
                            String message = queue.takeFirst();
                            try{
                                // Publish a message to the "color" exchange with the routing key currentPublishKey (can be "blue"  or "red")
                                ch.basicPublish(EXCHANGE_NAME, currentPublishKey, null, message.getBytes());
                                Log.d("", "[s] " + message);
                                ch.waitForConfirmsOrDie();
                            } catch (Exception e){
                                Log.d("","[f] " + message);
                                queue.putFirst(message);
                                throw e;
                            }
                        }
                    } catch (InterruptedException e) {
                        break;
                    } catch (Exception e) {
                        Log.d("", "Connection broken: " + e.getClass().getName());  //debug here
                        try {
                            Thread.sleep(5000); //sleep and then try again
                        } catch (InterruptedException e1) {
                            break;
                        }
                    }
                }
            }
        });
        publishThread.start();
    }
}


