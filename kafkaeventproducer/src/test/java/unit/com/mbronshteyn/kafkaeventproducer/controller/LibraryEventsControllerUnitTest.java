package com.mbronshteyn.kafkaeventproducer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mbronshteyn.kafkaeventproducer.domain.Book;
import com.mbronshteyn.kafkaeventproducer.domain.LibraryEvent;
import com.mbronshteyn.kafkaeventproducer.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
class LibraryEventsControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean
    LibraryEventProducer mockLibraryEventProducer;

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void postLibraryEvent() throws Exception {
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

        // mock
        Mockito.doNothing().when(mockLibraryEventProducer).sendLibraryEventProducerRecord(libraryEvent);

        String libraryEventPayload = objectMapper.writeValueAsString(libraryEvent);

        mockMvc.perform(
                        post("/v1/libraryevent")
                                .content(libraryEventPayload)
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());

        Mockito.verify(mockLibraryEventProducer, times(1))
                .sendLibraryEventProducerRecord(any(LibraryEvent.class));
    }

    @Test
    void postLibraryEvent_4xx_BookIsNull() throws Exception {
        //given
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(null)
                .build();

        // mock
        Mockito.doNothing().when(mockLibraryEventProducer).sendLibraryEventProducerRecord(libraryEvent);

        String libraryEventPayload = objectMapper.writeValueAsString(libraryEvent);

        mockMvc.perform(
                        post("/v1/libraryevent")
                                .content(libraryEventPayload)
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError());
    }

    @Test
    void postLibraryEvent_4xx_BookAuthorIsNull() throws Exception {

        Book book = Book.builder()
                .bookId(123)
                .bookAuthor(null)
                .bookName("Kafka using Spring Boot")
                .build();

        //given
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        // mock
        Mockito.doNothing().when(mockLibraryEventProducer).sendLibraryEventProducerRecord(libraryEvent);

        String libraryEventPayload = objectMapper.writeValueAsString(libraryEvent);

        String expectedErrorMessage = "book - must not be null";

        mockMvc.perform(
                        post("/v1/libraryevent")
                                .content(libraryEventPayload)
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorMessage));
    }
}