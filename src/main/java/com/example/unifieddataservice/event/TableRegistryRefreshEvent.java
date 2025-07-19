package com.example.unifieddataservice.event;

import org.springframework.context.ApplicationEvent;

public class TableRegistryRefreshEvent extends ApplicationEvent {
    public TableRegistryRefreshEvent(Object source) {
        super(source);
    }
}
