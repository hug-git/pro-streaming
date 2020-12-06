package com.hug.canal.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hug.canal.utils.MyKafkaSender;
import com.hug.mocker.constants.GmallConstant;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example", "", "");

        while (true) {
            canalConnector.connect();
            canalConnector.subscribe("gmall2020.*");

            // 抓取数据
            Message message = canalConnector.get(100);

            if (message.getEntries().size() <= 0) {
//                System.out.println("当前没数据，休息一下。。。");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //解析message
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //判断类型，如果非ROWDATA类型，则不解析
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        // 获取表名
                        String tableName = entry.getHeader().getTableName();
                        // 或崎岖数据
                        ByteString storeValue = entry.getStoreValue();
                        try {
                            // 反序列化数据
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            // 获取操作数据类型
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            // 获取数据集
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                            // 处理数据，根据数据类型及表名，将数据发送至Kafka
                            handler(tableName, eventType, rowDatasList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    /**
     * 根据数据类型及表名，将数据发送至Kafka
     *
     * @param tableName    表名
     * @param eventType    数据类型
     * @param rowDatasList 数据集
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        // 订单表，只需要新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstant.KAFKA_TOPIC_ORDER_INFO);
            // 订单明细表，只需要新增数据
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstant.KAFKA_TOPIC_ORDER_DETAIL);
            // 用户信息表，需要新增及变化数据
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            sendToKafka(rowDatasList, GmallConstant.KAFKA_TOPIC_USER_INFO);
        }
    }

    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }

//            try {
//                Thread.sleep(new Random().nextInt(5) * 1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            //打印数据，并将数据发送至Kafka
            System.out.println(jsonObject);
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}
