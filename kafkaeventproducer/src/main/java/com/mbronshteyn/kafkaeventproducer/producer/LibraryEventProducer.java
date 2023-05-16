package com.mbronshteyn.kafkaeventproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mbronshteyn.kafkaeventproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Value("${library-events-topic}")
    private String libraryEventsTopic;

    ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Send Library Event asynchronously
     *
     * @param libraryEvent
     * @throws JsonProcessingException
     */
    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        ListenableFuture<SendResult<Integer, String>> sendResult = kafkaTemplate.send(libraryEventsTopic, key, value);

        sendResult.addCallback(
                new ListenableFutureCallback<SendResult<Integer, String>>() {
                    @Override
                    public void onFailure(Throwable ex) {
                        handleError(key, value, ex);
                    }

                    @Override
                    public void onSuccess(SendResult<Integer, String> result) {
                        handleSuccess(key, value, result);
                    }
                }
        );

    }

    /**
     * Send event using ProducerRecord
     * This approach allows to pass headers to the topic message
     * Those headers could be used later to filter out which topic would receive the message
     *
     * @param libraryEvent
     * @throws JsonProcessingException
     */
    public ListenableFuture<SendResult<Integer, String>> sendLibraryEventProducerRecord(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(libraryEventsTopic, key, value);
        ListenableFuture<SendResult<Integer, String>> sendResult = kafkaTemplate.send(producerRecord);

        // just a sample code.  thus repeated more than once
        sendResult.addCallback(
                new ListenableFutureCallback<SendResult<Integer, String>>() {
                    @Override
                    public void onFailure(Throwable ex) {
                        handleError(key, value, ex);
                    }

                    @Override
                    public void onSuccess(SendResult<Integer, String> result) {
                        handleSuccess(key, value, result);
                    }
                }
        );

        return sendResult;
    }

    private ProducerRecord<Integer, String> buildProducerRecord(String topic, Integer key, String value) {

        List<Header> recordHeaders = List.of(new RecordHeader("event", "scanner".getBytes()));

        return new ProducerRecord<>(libraryEventsTopic, null, key, value, recordHeaders);
    }

    /**
     * This method will block until the call is acknowledged by brokers.
     * Will slow performance and should be used only if such blocking is necessary
     *
     * @param libraryEvent
     * @throws JsonProcessingException
     */
    public void sendLibraryEventSync(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        try {
            kafkaTemplate.send(libraryEventsTopic, key, value).get();
        } catch (InterruptedException | ExecutionException ex) {
            log.error("InterruptedException | ExecutionException : {}", ex.getMessage());
            throw ex;
        } catch (Exception e) {
            log.error("Exception : {}", e.getMessage());
            throw e;
        }
    }

    private void handleError(Integer key, String value, Throwable ex) {
        log.error("Message errored key : {}, value: {}, ex: {}",
                key, value, ex.getMessage());

        try {
            throw ex;
        } catch (Throwable th) {
            log.error("Error in OnFailure: {}", th.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message sent successfully for key: {}, value: {}, partition: {} ",
                key, value, result.getRecordMetadata().partition());
    }


}
