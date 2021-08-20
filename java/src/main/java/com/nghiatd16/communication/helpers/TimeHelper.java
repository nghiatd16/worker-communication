package com.nghiatd16.communication.helpers;

import java.time.LocalDateTime;
import java.util.Date;

public class TimeHelper {
    public static final TimeHelper Instance = new TimeHelper();

    private TimeHelper() {
    }

    public LocalDateTime getLocalNow() {
        LocalDateTime now = LocalDateTime.now();
        return now;
    }

    public long getLocalNowTimestamp() {
        return System.currentTimeMillis();
    }

    public int getCurrentHour() {
        LocalDateTime now = this.getLocalNow();
        return now.getHour();
    }

    public int getCurrentDay() {
        LocalDateTime now = this.getLocalNow();
        return now.getDayOfMonth();
    }

    public int getNextDay() {
        LocalDateTime now = this.getLocalNow();
        now.plusDays(1L);
        return now.getDayOfMonth();
    }

    public Date parseTimeStampSeconds(int ts) {
        return new Date((long)ts * 1000);
    }

    public Date parseTimeStampMiliSecs(long ts) {
        return new Date(ts);
    }
}
