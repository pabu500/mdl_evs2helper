package org.pabuff.evs2helper.event;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
public class OpResultPublisher {
    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

    public void publishEvent(final OpResultEvent event) {
        applicationEventPublisher.publishEvent(event);
    }
}
