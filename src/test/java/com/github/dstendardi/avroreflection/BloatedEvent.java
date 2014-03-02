package com.github.dstendardi.avroreflection;


import org.joda.money.Money;
import org.joda.time.DateTime;

public class BloatedEvent extends ParentEvent<Marker> {

    private final WrappedString<String> id;

    private final DateTime dateTime;

    private final Money money;

    public BloatedEvent(WrappedString<String> id, DateTime dateTime, Money money) {
        this.id = id;
        this.dateTime = dateTime;
        this.money = money;
    }

    public WrappedString<String> getId() {
        return id;
    }

    public DateTime getDateTime() {
        return dateTime;
    }

    public Money getMoney() {
        return money;
    }
}
