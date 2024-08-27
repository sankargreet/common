using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

class S3ZipFileExtractor
{
    private static readonly string bucketName = "your-bucket-name";
    private static readonly string keyName = "your-zip-file.zip";
    private static readonly string destinationFolder = "extracted-files/"; // S3 folder where files will be uploaded

    private static readonly AmazonS3Client s3Client = new AmazonS3Client();

    public static async Task Main(string[] args)
    {
        try
        {
            // Step 1: Download the ZIP file from S3 into a memory stream
            Console.WriteLine("Downloading the ZIP file from S3 into memory...");
            using (MemoryStream zipStream = new MemoryStream())
            {
                await DownloadZipFileToStream(zipStream);

                // Step 2: Unzip the files in memory and upload them to S3
                Console.WriteLine("Unzipping the file and uploading the contents to S3...");
                await UnzipAndUploadFiles(zipStream);
            }

            Console.WriteLine("Files unzipped and uploaded to S3 successfully.");
        }
        catch (AmazonS3Exception e)
        {
            Console.WriteLine($"Error processing the file from S3: {e.Message}");
        }
        catch (Exception e)
        {
            Console.WriteLine($"An error occurred: {e.Message}");
        }
    }

    private static async Task DownloadZipFileToStream(MemoryStream zipStream)
    {
        using (GetObjectResponse response = await s3Client.GetObjectAsync(bucketName, keyName))
        {
            await response.ResponseStream.CopyToAsync(zipStream);
            zipStream.Position = 0; // Reset stream position to the beginning
        }
        Console.WriteLine("ZIP file downloaded to memory.");
    }

    private static async Task UnzipAndUploadFiles(MemoryStream zipStream)
    {
        using (ZipArchive archive = new ZipArchive(zipStream, ZipArchiveMode.Read))
        {
            foreach (ZipArchiveEntry entry in archive.Entries)
            {
                Console.WriteLine($"Processing file: {entry.FullName}");
                
                using (Stream entryStream = entry.Open())
                {
                    string s3Key = destinationFolder + entry.FullName; // Define the destination path in S3
                    
                    var uploadRequest = new PutObjectRequest
                    {
                        BucketName = bucketName,
                        Key = s3Key,
                        InputStream = entryStream
                    };

                    // Upload each file extracted from the zip archive to S3
                    await s3Client.PutObjectAsync(uploadRequest);
                    Console.WriteLine($"Uploaded {entry.FullName} to S3 at {s3Key}");
                }
            }
        }
    }
}
