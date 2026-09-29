package com.marcosfurquim.iotcase.processing;

import com.marcosfurquim.iotcase.model.EventEnvelope;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class EventProcessor {
    private final EventClassifier classifier;
    private final ProcessedEventRepository repository;
    public EventProcessor(EventClassifier classifier, ProcessedEventRepository repository) {
        this.classifier = classifier;
        this.repository = repository;
    }

    @Transactional
    public boolean process(EventEnvelope event) {
        Classification classification = classifier.classify(event);
        boolean inserted = repository.insertIfAbsent(event, classification);
        return inserted;
    }
}
