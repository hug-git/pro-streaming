package com.hug.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

public interface PublisherService {

    int getDauTotal(String date);

    Map getDauHourTotal(String date);

    Double getOrderAmount(String date);

    Map getOrderAmountHour(String date);

    public String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException;

}
