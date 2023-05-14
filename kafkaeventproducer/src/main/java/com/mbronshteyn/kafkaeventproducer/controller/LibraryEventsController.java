package com.mbronshteyn.kafkaeventproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mbronshteyn.kafkaeventproducer.domain.LibraryEvent;
import com.mbronshteyn.kafkaeventproducer.domain.LibraryEventType;
import com.mbronshteyn.kafkaeventproducer.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LibraryEventsController {

    @Autowired
    private LibraryEventProducer libraryEventProducer;

    @PostMapping( "/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

        log.info("creating library event {}", libraryEvent);

        libraryEvent.setEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEventProducerRecord(libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED.value())
                .body(libraryEvent);
    }

    @GetMapping("/v1/libraryevent")
    public ResponseEntity<String> getLibraryEvent() {
        return ResponseEntity.ok("Hello World!!!");
    }
}
