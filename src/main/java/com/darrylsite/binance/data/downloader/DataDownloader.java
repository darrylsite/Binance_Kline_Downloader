package com.darrylsite.binance.data.downloader;

import net.lingala.zip4j.ZipFile;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class DataDownloader {
    private static final String DOWNLOAD_URL = "https://data.binance.vision/data/futures/um/monthly/klines/{currencyPair}/{timeFrame}/";

    public static List<String> download(String currencyPair, String timeFrame, int maxMonth) throws IOException {
        String dateFormatPattern = "YYYY_MM_dd";
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern(dateFormatPattern);
        String dateStr = dateFormat.format(LocalDate.now());
        String folderName = currencyPair + "_"+ dateStr;
        //Create the directory for holder the zip files
        Files.createDirectory(Paths.get(folderName));

        //Remote URL
        String urlPrefix = DOWNLOAD_URL.replace("{currencyPair}", currencyPair).replace("{timeFrame}", timeFrame);
        String shortDateFormatPattern = "YYYY-MM";
        String fileNamePrefix = currencyPair+ '-' + timeFrame + '-';

        List<String> downloadedFiles = new ArrayList<>();

        for(int i=1; i <=maxMonth; i++){
            LocalDate localDate = LocalDate.now().minusMonths(i);
            String remoteFileName = fileNamePrefix + DateTimeFormatter.ofPattern(shortDateFormatPattern).format(localDate);
            remoteFileName = remoteFileName + ".zip";
            String url = urlPrefix + remoteFileName;

            try {
                System.out.println("... Downloading : " + remoteFileName);
                String localFileName = folderName + File.separator + remoteFileName;
                FileUtils.copyURLToFile(new URL(url), new File(localFileName));
                downloadedFiles.add(localFileName);
            } catch(IOException ex){
                break;
            }
        }

        return downloadedFiles;
    }

    private static void mergeDownloadedFiles(String currencyPair, String timeFrame, List<String> downloadedFiles) throws FileNotFoundException {
        String folder = downloadedFiles.get(0).split(File.separator)[0];
        String mergedFileName = currencyPair + "-" + timeFrame + "-merged.csv";
        mergedFileName = folder + File.separator + mergedFileName;
        PrintWriter printWriter = new PrintWriter(mergedFileName);

        downloadedFiles.stream().sorted().forEach(zipFileName->{
            try {
                new ZipFile(zipFileName).extractAll(folder);
                String csvFileName = zipFileName.replace(".zip", ".csv");

                BufferedReader br = new BufferedReader(new FileReader(csvFileName));
                String line = br.readLine();

                while (line != null)
                {
                    printWriter.println(line);
                    line = br.readLine();
                }

                br.close();
                printWriter.flush();
                System.out.println("... Merged : " + csvFileName);

                //Delete the zip and csv file
                Files.delete(Paths.get(csvFileName));
                Files.delete(Paths.get(zipFileName));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        printWriter.flush();
        printWriter.close();

        System.out.println("... All the files was merged to : " + mergedFileName);
    }

    public static void main(String[] args) throws IOException {
        String currencyPair = "WAVESUSDT";
        String timeFrame = "5m";
        int maxMonths = 12;

        List<String> downloadedFiles = download(currencyPair, timeFrame, maxMonths);
        if(downloadedFiles.isEmpty()){
            System.out.println("No files where downloaded");
            return;
        }

        mergeDownloadedFiles(currencyPair, timeFrame, downloadedFiles);
    }
}
