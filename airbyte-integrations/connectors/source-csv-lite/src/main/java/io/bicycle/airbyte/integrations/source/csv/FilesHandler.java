package io.bicycle.airbyte.integrations.source.csv;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilesHandler {
    private static final Logger logger = LoggerFactory.getLogger(FilesHandler.class.getName());
    public static Map<String, File> getCSVFiles(String name, File inputFile) throws IOException {
        Map<String, File> csvFiles = new HashMap<>();

        String fileName = inputFile.getName();
        String fileExtension = fileName.substring(fileName.lastIndexOf('.') + 1);

        if (fileExtension.equals("gz")) {
            // Decompress .gz file
            File decompressedFile = decompressGzip(inputFile);
            String decompressedFileName = decompressedFile.getName();
            String decompressedFileExtension = decompressedFileName.substring(decompressedFileName.lastIndexOf('.') + 1);

            if (decompressedFileExtension.equals("gz")) {
                // Recursively call getCSVFiles for the decompressed .gz file
                csvFiles.putAll(getCSVFiles(decompressedFileName, decompressedFile));
            } else {
                csvFiles.put(decompressedFile.getName(), decompressedFile);
            }
        } else if (fileExtension.equals("zip")) {
            // Extract .zip file
            //csvFiles.addAll(extractZip(inputFile));
        } else if (fileExtension.equals("tar")) {
            // Extract .tar file
            //csvFiles.addAll(extractTar(inputFile));
        } else if (fileExtension.equals("csv")) {
            csvFiles.put(name, inputFile);
        } else if (fileExtension.equals("json")) {
            csvFiles.put(name, inputFile);
        }

        return csvFiles;
    }

    private static File decompressGzip(File inputFile) throws IOException {
        File outputFile = new File(inputFile.getParent(), inputFile.getName().replace(".gz", ""));
        logger.info("Input file to decompress is {} and output file is {}", inputFile.getPath(), outputFile.getPath());
        try (
                FileInputStream fis = new FileInputStream(inputFile);
                GZIPInputStream gis = new GZIPInputStream(fis);
                FileOutputStream fos = new FileOutputStream(outputFile);
        ) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) != -1) {
                fos.write(buffer, 0, len);
            }
        }
        return outputFile;
    }

}
