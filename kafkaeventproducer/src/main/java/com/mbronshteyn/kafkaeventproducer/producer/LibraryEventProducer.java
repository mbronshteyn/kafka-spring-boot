package com.mbronshteyn.kafkaeventproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mbronshteyn.kafkaeventproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    ObjectMapper objectMapper = new ObjectMapper();

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        ListenableFuture<SendResult<Integer, String>> sendResult = kafkaTemplate.send("library-events", key, value);

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
