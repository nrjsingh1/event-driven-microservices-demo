package com.nrjsingh1.microservices.twitterToKafkaService.runner.impl;

import com.nrjsingh1.microservices.twitterToKafkaService.config.TwitterToKafkaServiceConfigData;
import com.nrjsingh1.microservices.twitterToKafkaService.listener.TwitterKafkaStatusListener;
import com.nrjsingh1.microservices.twitterToKafkaService.runner.StreamRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private final TwitterKafkaStatusListener statusListener;
    private final TwitterToKafkaServiceConfigData configData;
    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(TwitterKafkaStatusListener statusListener,
                                    TwitterToKafkaServiceConfigData configData) {
        this.statusListener = statusListener;
        this.configData = configData;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(statusListener);
        twitterStream.sample();
        addFilter();
    }

    @PreDestroy
    public void shutDown(){
        if(twitterStream!=null){
            LOG.info("Closing twitter stream!");
            twitterStream.shutdown();
        }
    }
    private void addFilter() {
        String[] keywords = configData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOG.info("Started filtering twitter stream for keywords {}", Arrays.toString(keywords));
    }
}
