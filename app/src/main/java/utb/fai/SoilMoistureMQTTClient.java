package utb.fai;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import utb.fai.API.HumiditySensor;
import utb.fai.API.IrrigationSystem;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Trida MQTT klienta pro mereni vhlkosti pudy a rizeni zavlazovaciho systemu. 
 * 
 * V teto tride implementuje MQTT klienta
 */
public class SoilMoistureMQTTClient {

    private MqttClient client;
    private HumiditySensor humiditySensor;
    private IrrigationSystem irrigationSystem;
    private ScheduledFuture<?> irrigationStopper;
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /**
     * Vytvori instacni tridy MQTT klienta pro mereni vhlkosti pudy a rizeni
     * zavlazovaciho systemu
     *
     * @param sensor     Senzor vlhkosti
     * @param irrigation Zarizeni pro zavlahu pudy
     */
    public SoilMoistureMQTTClient(HumiditySensor sensor, IrrigationSystem irrigation) {
        this.humiditySensor = sensor;
        this.irrigationSystem = irrigation;
    }

    /**
     * Metoda pro spusteni klienta
     */
    public void start() {
        try {
            client = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId(), new MemoryPersistence());
            client.connect();
            System.out.println("Connected to MQTT broker");

            // Subscribe to the topic for incoming messages
            client.subscribe("topic/device1/in", (topic, message) -> {
                String payload = new String(message.getPayload());
                handleIncomingMessage(payload);
            });

            // Start sending humidity data
            startSendingHumidityData();

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }


    private void handleIncomingMessage(String message) throws MqttException {
        if (message.startsWith("get-humidity")) {
            sendHumidity();
        } else if (message.startsWith("get-status")) {
            sendStatus();
        } else if (message.startsWith("start-irrigation")) {
            startWatering();
        } else if (message.startsWith("stop-irrigation")) {
            stopWatering();
        }
    }

    private void startSendingHumidityData() {
        scheduler.scheduleAtFixedRate(() -> {

            try {
                if (humiditySensor.hasFault() || humiditySensor.readRAWValue() < 0) {
                    msg_handleError("HUMIDITY_SENSOR");
                } else {
                    String humidity = "humidity;" + humiditySensor.readRAWValue();
                    client.publish("topic/device1/out", new MqttMessage(humidity.getBytes()));
                }
            } catch (Exception e) {
                try {
                    msg_handleError("HUMIDITY_SENSOR");
                } catch (MqttException ex) {
                    ex.printStackTrace();
                }
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    private void sendHumidity() throws MqttException {
        try {
            String humidity = "humidity;" + humiditySensor.readRAWValue();
            client.publish("topic/device1/out", new MqttMessage(humidity.getBytes()));
        } catch (MqttException e) {
            msg_handleError("HUMIDITY_SENSOR");
        }
    }


    private void sendStatus() throws MqttException {
        try {
            String status = irrigationSystem.isActive() ? "status;irrigation_on" : "status;irrigation_off";
            client.publish("topic/device1/out", new MqttMessage(status.getBytes()));
        } catch (MqttException e) {
            msg_handleError("IRRIGATION_SYSTEM");
        }
    }

    private void startWatering() throws MqttException {
        try {
            if (irrigationSystem.hasFault()) {
                msg_handleError("IRRIGATION_SYSTEM");
            } else {
                irrigationSystem.activate();
                msg_handleWateringActivation();
                resetWateringStopper();
            }
        } catch (Exception e) {
            msg_handleError("IRRIGATION_SYSTEM");
        }
    }

    private void stopWatering() throws MqttException {
        try {
            if (irrigationSystem.hasFault()) {
                msg_handleError("IRRIGATION_SYSTEM");
            } else {
                irrigationSystem.deactivate();
                msg_handleWateringDeactivation();
                if (irrigationStopper != null) {
                    irrigationStopper.cancel(false);
                }
            }
        } catch (Exception e) {
            msg_handleError("IRRIGATION_SYSTEM");
        }
    }

    private void resetWateringStopper() {
        if (irrigationStopper != null) {
            irrigationStopper.cancel(false);
        }
        irrigationStopper = scheduler.schedule(() -> {
            try {
                stopWatering();
            } catch (MqttException e) {
                try {
                    msg_handleError("IRRIGATION_SYSTEM");
                } catch (MqttException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }, 30, TimeUnit.SECONDS);
    }

    private void msg_handleWateringActivation() throws MqttException {
        if (irrigationSystem.hasFault()){
            msg_handleError("IRRIGATION_SYSTEM");
        } else if (humiditySensor.hasFault()) {
            msg_handleError("HUMIDITY_SENSOR");

        }
        else{
            String status = "status;irrigation_on";
            client.publish("topic/device1/out", new MqttMessage(status.getBytes()));
        }}

    private void msg_handleWateringDeactivation() throws MqttException {
        if (irrigationSystem.hasFault()){
            msg_handleError("IRRIGATION_SYSTEM");
        } else if (humiditySensor.hasFault()) {
            msg_handleError("HUMIDITY_SENSOR");

        }
        else{
            String status = "status;irrigation_off";
            client.publish("topic/device1/out", new MqttMessage(status.getBytes()));
        }}

    private void msg_handleError(String e) throws MqttException {
        String error = "fault;" + e;
        client.publish("topic/device1/out", new MqttMessage(error.getBytes()));
    }

}
