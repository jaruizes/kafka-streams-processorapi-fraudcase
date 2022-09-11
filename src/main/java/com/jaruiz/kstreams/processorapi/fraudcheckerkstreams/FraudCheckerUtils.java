package com.jaruiz.kstreams.processorapi.fraudcheckerkstreams;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class FraudCheckerUtils {

    public static long iso8601ToEpoch(String createdAt) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        try {
            return formatter.parse(createdAt).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

}