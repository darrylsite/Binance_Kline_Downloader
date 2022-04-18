# Binance Kline Downloader

Code for downloading Binance future Kline data.
The program use the monthly Kline data available at : https://data.binance.vision/?prefix=data/futures/um/monthly/klines/
First the zip files are downloaded then the CSV files are merged all together.

```java
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
```