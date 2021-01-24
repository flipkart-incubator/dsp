package com.flipkart.dsp.utils;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDate;
import java.time.temporal.WeekFields;
import java.util.Locale;

/**
 */

@Data
@AllArgsConstructor
public class YearWeek {
    private Integer year;
    private Integer week;

    public static YearWeek currentWeek(){
        return getWeekyear(0);
    }

    public static YearWeek getWeekyear(Integer weekDiffFromCurrent){
        LocalDate localDate = LocalDate.now();
        localDate = localDate.plusWeeks(weekDiffFromCurrent);
        return YearWeek.parseFromDate(localDate);
    }

    public static YearWeek parseYYYYWW(String yearWeek){
        Integer year = Integer.valueOf(yearWeek.substring(0, 4));
        Integer week = Integer.valueOf(yearWeek.substring(4));
        return new YearWeek(year, week);
    }

    public static YearWeek parseYYYYwWW(String yearwWeek){
        String[] yearWeekArray = yearwWeek.split("w");
        Integer year = Integer.valueOf(yearWeekArray[0]);
        Integer week = Integer.valueOf(yearWeekArray[1]);
        return new YearWeek(year, week);
    }

    /*
        return date corresponding to today's day(sunday/monday etc.) in given year week
    */
    public LocalDate getDate(){
        WeekFields weekfields = WeekFields.of(Locale.getDefault());
        return LocalDate.now().with(weekfields.weekOfWeekBasedYear(), week).with(weekfields.weekBasedYear(), year);
    }

    public long diffWeek(YearWeek yearWeek){
        return DateUtils.getWeekDiffBetweenDates(this.getDate(), yearWeek.getDate());
    }

    @Override
    public String toString() {
        return String.format("%d%02d", year, week);
    }

    public int compareTo(YearWeek yearWeek) {
        return (this.year.equals(yearWeek.year))? this.week.compareTo(yearWeek.getWeek()) : this.year.compareTo(yearWeek.getYear());
    }

    public YearWeek minusWeek(long lagWeek) {
        LocalDate date1 = this.getDate().minusWeeks(lagWeek);
        return YearWeek.parseFromDate(date1);
    }

    public YearWeek plusWeek(long lagWeek) {
        LocalDate date1 = this.getDate().plusWeeks(lagWeek);
        return YearWeek.parseFromDate(date1);
    }

    private static YearWeek parseFromDate(LocalDate localDate) {
        WeekFields weekfields = WeekFields.of(Locale.getDefault());
        Integer yearOfWeekBasedYear = localDate.get(weekfields.weekBasedYear());
        Integer weekOfWeekBasedYear = localDate.get(weekfields.weekOfWeekBasedYear());
        return new YearWeek(yearOfWeekBasedYear, weekOfWeekBasedYear);
    }

    public static void main(String[] args) {
        LocalDate localDate = LocalDate.parse("2014-12-28");
        YearWeek yearWeek = YearWeek.parseFromDate(localDate);
        LocalDate localDate1 = yearWeek.getDate();
        System.out.println(yearWeek);
        System.out.println(localDate1);
    }
}
