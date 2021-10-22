package my.diploma.thesis.consumer;

import com.google.gson.JsonParser;
import my.diploma.thesis.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TwitterConsumer extends Thread {

    private final XSSFWorkbook workbook = new XSSFWorkbook();
    private final String filePath;
    private final String filename;

    public TwitterConsumer(String filePath, String filename) {
        this.filePath = filePath;
        this.filename = filename;
    }

    @Override
    public void run() {
        Logger logger = LoggerFactory.getLogger(TwitterConsumer.class.getName());

        String groupId = "my-first-app";
        String topic = "info";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(
                groupId,
                topic,
                latch
        );

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info(" ---- Application has exited ---- ");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info(" ---- Application is closing ---- ");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;

            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            // create consumer
            consumer = new KafkaConsumer<>(properties);
            // subscribe consumer to our topic(s)
            consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {

            Sheet sheet = workbook.createSheet();
            sheet.setColumnWidth(0, 5000);
            sheet.setColumnWidth(1, 15000);
            sheet.setColumnWidth(2, 8000);

            Row header = sheet.createRow(0);

            CellStyle headerStyle = workbook.createCellStyle();
            headerStyle.setFillForegroundColor(IndexedColors.LIGHT_GREEN.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            headerStyle.setBorderBottom(BorderStyle.THIN);
            headerStyle.setBorderRight(BorderStyle.THIN);

            XSSFFont font = workbook.createFont();
            font.setFontName("Arial");
            font.setFontHeightInPoints((short) 16);
            font.setBold(true);
            headerStyle.setFont(font);
            headerStyle.setAlignment(HorizontalAlignment.CENTER);
            headerStyle.setVerticalAlignment(VerticalAlignment.CENTER);

            CellStyle style = workbook.createCellStyle();
            style.setWrapText(true);
            style.setAlignment(HorizontalAlignment.CENTER);
            style.setVerticalAlignment(VerticalAlignment.CENTER);

            // 1st column
            Cell headerCell = header.createCell(0);
            headerCell.setCellValue("Username");
            headerCell.setCellStyle(headerStyle);

            // 2nd column
            headerCell = header.createCell(1);
            headerCell.setCellValue("Tweet");
            headerCell.setCellStyle(headerStyle);

            // 3rd column
            headerCell = header.createCell(2);
            headerCell.setCellValue("Created at");
            headerCell.setCellStyle(headerStyle);

            // poll for new data
            try {
                int i = 1;
                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

                    if (records.isEmpty()) break;

                    for (ConsumerRecord<String, String> record : records) {

                        Row row = sheet.createRow(i++);
                        try {
                            Cell cell = row.createCell(0);
                            cell.setCellValue("@" + JsonParser.parseString(record.value())
                                    .getAsJsonObject()
                                    .get("username")
                                    .getAsString());
                            cell.setCellStyle(style);

                            cell = row.createCell(1);
                            cell.setCellValue(JsonParser.parseString(record.value())
                                    .getAsJsonObject()
                                    .get("tweet")
                                    .getAsString());
                            cell.setCellStyle(style);

                            cell = row.createCell(2);
                            cell.setCellValue(JsonParser.parseString(record.value())
                                    .getAsJsonObject()
                                    .get("created_at")
                                    .getAsString());
                            cell.setCellStyle(style);
                            System.out.println("Done");
                        } catch (Exception e) {
                            sheet.removeRow(row);
                            i--;
                            System.out.println("passed");
//                            logger.error("Exception transforming json: ", e);
                        }
                    }
                    sheet.removeRow(sheet.getRow(1));
                    sheet.shiftRows(2, i, -1);
                }
            } catch (WakeupException e) {
                logger.info(" ----- Received shutdown signal! ----- ");
            } finally {
                consumer.close();
                // tell our main code we're done with the consumer
                latch.countDown();
            }

            String fileLocation = filePath + "\\" + filename + ".xlsx";

            try {
                FileOutputStream outputStream = new FileOutputStream(fileLocation);
                workbook.write(outputStream);
                workbook.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }

}
