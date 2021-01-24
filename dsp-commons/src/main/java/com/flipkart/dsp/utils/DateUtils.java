package com.flipkart.dsp.utils;

import org.joda.time.DateTimeConstants;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.IsoFields;

import static org.joda.time.DateTimeConstants.DAYS_PER_WEEK;

/**
 */
public class DateUtils {
    public static final long DAYS_PER_WEEK = DateTimeConstants.DAYS_PER_WEEK;

    public static String getDate(Integer dayFromCurrent, String dayFormat) {
        LocalDate localDate = LocalDate.now();
        localDate = localDate.plusDays(dayFromCurrent);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dayFormat);
        return localDate.format(formatter);
    }

    public static String getDate(Integer dayFromCurrent) {
        String dayFormat = "yyyy-MM-dd";
        return DateUtils.getDate(dayFromCurrent, dayFormat);
    }

    public static long getWeekDiffBetweenDates(LocalDate startDate, LocalDate endDate){
        long days = DateUtils.getDaysDiffBetweenDates(startDate, endDate);
        return days/DAYS_PER_WEEK;
    }

    public static long getDaysDiffBetweenDates(LocalDate startDate, LocalDate endDate){
        return endDate.toEpochDay() - startDate.toEpochDay();
    }
}
