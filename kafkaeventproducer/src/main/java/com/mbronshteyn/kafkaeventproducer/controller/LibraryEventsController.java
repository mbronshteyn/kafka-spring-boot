package com.mbronshteyn.kafkaeventproducer.controller;

import com.mbronshteyn.kafkaeventproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LibraryEventsController {

    @PostMapping( "/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) {

        // invoke Kafka producer

        log.info("creating library event {}", libraryEvent);

        libraryEvent.setLibraryEventId(1);

        return ResponseEntity.status(HttpStatus.CREATED.value())
                .body(libraryEvent);
    }

    @GetMapping("/v1/libraryevent")
    public ResponseEntity<String> getLibraryEvent() {
        return ResponseEntity.ok("Hello World!!!");
    }
}
