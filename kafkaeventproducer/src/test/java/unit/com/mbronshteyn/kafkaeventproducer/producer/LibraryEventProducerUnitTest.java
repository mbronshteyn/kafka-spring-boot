package com.mbronshteyn.kafkaeventproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mbronshteyn.kafkaeventproducer.domain.Book;
import com.mbronshteyn.kafkaeventproducer.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @InjectMocks
    LibraryEventProducer eventProducer;

    @Test
    void sendLibraryEvent_ProducerRecord_failure() throws JsonProcessingException, ExecutionException, InterruptedException {
        //given
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Mike")
                .bookName("Kafka using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception Calling Kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        // set topic
        eventProducer.setLibraryEventsTopic("library-events");

        assertThrows(Exception.class,
                () -> eventProducer.sendLibraryEventProducerRecord(libraryEvent).get());
    }
}
