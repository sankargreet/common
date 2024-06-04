import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class S3FileChunkDownloader {

    private static final String BUCKET_NAME = "your-bucket-name";
    private static final String OBJECT_KEY = "your-large-file.txt";
    private static final Region REGION = Region.US_EAST_1; // Replace with your region
    private static final long CHUNK_SIZE = 512 * 1024 * 1024; // 0.5 GB in bytes

    public static void main(String[] args) throws IOException {
        S3Client s3Client = S3Client.builder()
                .region(REGION)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();

        downloadFileInChunks(s3Client);
    }

    private static void downloadFileInChunks(S3Client s3Client) throws IOException {
        long fileSize = getFileSize(s3Client);
        long totalBytesRead = 0;
        int chunkNumber = 0;

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(OBJECT_KEY)
                .build();

        try (InputStream responseStream = s3Client.getObject(getObjectRequest)) {
            while (totalBytesRead < fileSize) {
                String chunkFilePath = "chunk_" + chunkNumber + ".txt";
                long bytesToRead = Math.min(CHUNK_SIZE, fileSize - totalBytesRead);

                long bytesReadInChunk = writeChunkToFile(responseStream, chunkFilePath, bytesToRead);

                totalBytesRead += bytesReadInChunk;
                chunkNumber++;

                if (bytesReadInChunk == 0) {
                    break;
                }
            }
        }

        System.out.println("Chunks downloaded successfully.");
    }

    private static long getFileSize(S3Client s3Client) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(OBJECT_KEY)
                .build();

        HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);
        return headObjectResponse.contentLength();
    }

    private static long writeChunkToFile(InputStream responseStream, String chunkFilePath, long bytesToRead) throws IOException {
        long bytesReadInChunk = 0;
        long lastNewlinePosition = 0;
        byte[] buffer = new byte[8192];

        try (FileOutputStream fileOutputStream = new FileOutputStream(chunkFilePath)) {
            int bytesRead;
            while (bytesReadInChunk < bytesToRead && (bytesRead = responseStream.read(buffer)) > 0) {
                bytesReadInChunk += bytesRead;

                for (int i = 0; i < bytesRead; i++) {
                    if (buffer[i] == '\n') {
                        lastNewlinePosition = bytesReadInChunk - (bytesRead - i - 1);
                    }
                }

                fileOutputStream.write(buffer, 0, bytesRead);
            }
        }

        if (lastNewlinePosition < bytesReadInChunk) {
            adjustFileToLastNewline(chunkFilePath, lastNewlinePosition);
            bytesReadInChunk = lastNewlinePosition;
        }

        return bytesReadInChunk;
    }

    private static void adjustFileToLastNewline(String filePath, long position) throws IOException {
        try (FileOutputStream fileOutputStream = new FileOutputStream(filePath, true)) {
            fileOutputStream.getChannel().truncate(position);
        }
    }
}
