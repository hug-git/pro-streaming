package com.hug.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    //1 查询当日交易额总数
    Double selectOrderAmountTotal(String date);
    //2 查询当日交易额分时明细
    List<Map> selectOrderAmountHourMap(String date);
}
