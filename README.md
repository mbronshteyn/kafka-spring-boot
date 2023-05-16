# kafka-spring-boot

## kafkaeventproducer

This module shows how to use Spring KafkaTemplate to produce messages to Kafka topic. Class contains few different
options on how to call send method

There is an integration test which is using Spring Test Rest Template ( alternative to RestAssured ) to create POST
method and send message to Embeded Kafka.

LibraryEventsController also has unit test which is written with MockMVC.