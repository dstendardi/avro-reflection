package com.github.dstendardi.avroreflection;


import org.joda.money.Money;
import org.joda.time.DateTime;
import org.junit.Test;

public class AvroReflectionTest {


    @Test
    public void nominal() throws Exception {

        BloatedEvent event = new BloatedEvent(new WrappedString<String>("boo"), new DateTime("2013-12-10"), Money.parse("USD 23.87"));

        // good luck !!

    }
}
