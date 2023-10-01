package com.nrjsingh1.microservices.twitterToKafkaService.runner;

import twitter4j.TwitterException;

public interface StreamRunner {
    void start() throws TwitterException;
}
