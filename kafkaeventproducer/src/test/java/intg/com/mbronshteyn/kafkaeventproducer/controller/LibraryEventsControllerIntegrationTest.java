package com.mbronshteyn.kafkaeventproducer.controller;

import com.mbronshteyn.kafkaeventproducer.domain.Book;
import com.mbronshteyn.kafkaeventproducer.domain.LibraryEvent;
import com.mbronshteyn.kafkaeventproducer.domain.LibraryEventType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LibraryEventsControllerIntegrationTest {

    @Autowired
    TestRestTemplate testRestTemplate;
    @Test
    void postLibraryEvent() {

        // given
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Joe Doe")
                .bookName("Testng Kafka")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .eventType(LibraryEventType.NEW)
                .book(book)
                .build();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.CONTENT_TYPE,
                MediaType.APPLICATION_JSON.toString());

        HttpEntity<LibraryEvent> httpEntity = new HttpEntity<>(libraryEvent, httpHeaders);

        // when
        ResponseEntity<LibraryEvent> responseEntity = testRestTemplate.exchange("/v1/libraryevent", HttpMethod.POST,
                httpEntity, LibraryEvent.class);

        // verify
        Assertions.assertEquals(HttpStatus.CREATED,responseEntity.getStatusCode());
    }
}