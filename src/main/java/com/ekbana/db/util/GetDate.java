package com.ekbana.db.util;

import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class GetDate {

    private Logger logger = Logger.getLogger(GetDate.class);
    private final DateFormat dateFormat = new SimpleDateFormat("dd-MMM-yy");
//    private final DateFormat dateFormat_tch = new SimpleDateFormat("yyyy-MM-dd");
    private Calendar cal = Calendar.getInstance();

    public String addDate(Date date, int days){
        try {

            cal.setTime(date);
            cal.add(Calendar.DATE, days);
        }catch (Exception e){
            e.printStackTrace();
        }
        return dateFormat.format(cal.getTime());
    }

    public String addDate(String date, int days){
//        System.out.println("add date 30 = "+date);
        try {
            Date parsedDate = dateFormat.parse(date);
            cal.setTime(parsedDate);
            cal.add(Calendar.DATE, days);
        } catch (ParseException e) {
            e.printStackTrace();
//            logger.error(e);
        }
        return dateFormat.format(cal.getTime());
    }

    public String getFormatedDate(Date date){
        cal.setTime(date);
        return dateFormat.format(cal.getTime());
    }

    public String getFormatedDate(Date date, String date_format){
        cal.setTime(date);
        return new SimpleDateFormat(date_format).format(cal.getTime());
    }
}
