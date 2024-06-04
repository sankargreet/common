using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

class S3FileChunkDownloader
{
    private static readonly string bucketName = "your-bucket-name";
    private static readonly string objectKey = "your-large-file.txt";
    private static readonly string region = "us-east-1"; // Replace with your region
    private static readonly long chunkSize = 512 * 1024 * 1024; // 0.5 GB in bytes

    public static async Task Main(string[] args)
    {
        var s3Client = new AmazonS3Client(RegionEndpoint.GetBySystemName(region));
        await DownloadFileInChunksAsync(s3Client);
    }

    private static async Task DownloadFileInChunksAsync(AmazonS3Client s3Client)
    {
        long fileSize = await GetFileSizeAsync(s3Client);
        long totalBytesRead = 0;
        int chunkNumber = 0;

        using (var response = await s3Client.GetObjectAsync(bucketName, objectKey))
        using (var responseStream = response.ResponseStream)
        {
            while (totalBytesRead < fileSize)
            {
                string chunkFilePath = $"chunk_{chunkNumber}.txt";
                long bytesReadInChunk = await WriteChunkToFile(responseStream, chunkFilePath, chunkSize);

                long lastNewlinePosition = GetLastNewlinePosition(chunkFilePath);
                if (lastNewlinePosition < bytesReadInChunk)
                {
                    AdjustFileToLastNewline(chunkFilePath, lastNewlinePosition);
                    totalBytesRead += lastNewlinePosition;
                }
                else
                {
                    totalBytesRead += bytesReadInChunk;
                }

                chunkNumber++;
            }
        }

        Console.WriteLine("Chunks downloaded successfully.");
    }

    private static async Task<long> GetFileSizeAsync(AmazonS3Client s3Client)
    {
        var metadataRequest = new GetObjectMetadataRequest
        {
            BucketName = bucketName,
            Key = objectKey
        };

        var metadataResponse = await s3Client.GetObjectMetadataAsync(metadataRequest);
        return metadataResponse.ContentLength;
    }

    private static async Task<long> WriteChunkToFile(Stream responseStream, string chunkFilePath, long bytesToRead)
    {
        long bytesReadInChunk = 0;
        byte[] buffer = new byte[8192];

        using (var fileStream = new FileStream(chunkFilePath, FileMode.Create, FileAccess.Write))
        {
            int bytesRead;
            while (bytesReadInChunk < bytesToRead && (bytesRead = await responseStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
            {
                bytesReadInChunk += bytesRead;
                await fileStream.WriteAsync(buffer, 0, bytesRead);
            }
        }

        return bytesReadInChunk;
    }

    private static long GetLastNewlinePosition(string filePath)
    {
        long position = 0;
        using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
        using (var reader = new StreamReader(fileStream, Encoding.UTF8))
        {
            char[] buffer = new char[8192];
            int charsRead;
            while ((charsRead = reader.Read(buffer, 0, buffer.Length)) > 0)
            {
                for (int i = charsRead - 1; i >= 0; i--)
                {
                    if (buffer[i] == '\n')
                    {
                        position = fileStream.Position - (charsRead - i - 1);
                        return position;
                    }
                }
            }
        }

        return position;
    }

    private static void AdjustFileToLastNewline(string filePath, long position)
    {
        using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Write))
        {
            fileStream.SetLength(position);
        }
    }
}
