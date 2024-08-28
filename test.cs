using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

class S3FileZipper
{
    private static readonly string bucketName = "your-bucket-name";
    private static readonly List<string> fileKeys = new List<string>
    {
        "file1.txt",
        "file2.txt",
        "file3.txt"
    };
    private static readonly string localDownloadPath = "/tmp/s3-files/"; // Local path in the pod to store downloaded files
    private static readonly string zipFileName = "zipped-files.zip";
    private static readonly string zipFilePath = "/tmp/" + zipFileName;
    private static readonly string destinationZipKey = "zipped-folder/" + zipFileName; // S3 key where the zip file will be uploaded

    private static readonly AmazonS3Client s3Client = new AmazonS3Client();

    public static async Task Main(string[] args)
    {
        try
        {
            // Step 1: Download multiple files from S3 to the local storage in the pod
            Console.WriteLine("Downloading files from S3...");
            await DownloadFilesFromS3();

            // Step 2: Zip the downloaded files
            Console.WriteLine("Zipping the files...");
            ZipFiles();

            // Step 3: Upload the zip file back to S3
            Console.WriteLine("Uploading the zip file to S3...");
            await UploadZipFileToS3();

            Console.WriteLine("Files successfully zipped and uploaded to S3.");
        }
        catch (AmazonS3Exception e)
        {
            Console.WriteLine($"Error interacting with S3: {e.Message}");
        }
        catch (Exception e)
        {
            Console.WriteLine($"An error occurred: {e.Message}");
        }
    }

    private static async Task DownloadFilesFromS3()
    {
        Directory.CreateDirectory(localDownloadPath); // Ensure the local directory exists

        foreach (var fileKey in fileKeys)
        {
            string localFilePath = Path.Combine(localDownloadPath, Path.GetFileName(fileKey));

            using (GetObjectResponse response = await s3Client.GetObjectAsync(bucketName, fileKey))
            using (Stream responseStream = response.ResponseStream)
            using (FileStream fileStream = new FileStream(localFilePath, FileMode.Create, FileAccess.Write))
            {
                await responseStream.CopyToAsync(fileStream);
                Console.WriteLine($"Downloaded {fileKey} to {localFilePath}");
            }
        }
    }

    private static void ZipFiles()
    {
        if (File.Exists(zipFilePath))
        {
            File.Delete(zipFilePath); // Clean up if the zip file already exists
        }

        ZipFile.CreateFromDirectory(localDownloadPath, zipFilePath);
        Console.WriteLine($"Files zipped to {zipFilePath}");
    }

    private static async Task UploadZipFileToS3()
    {
        using (FileStream zipFileStream = new FileStream(zipFilePath, FileMode.Open, FileAccess.Read))
        {
            var uploadRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = destinationZipKey,
                InputStream = zipFileStream
            };

            await s3Client.PutObjectAsync(uploadRequest);
            Console.WriteLine($"Uploaded {zipFileName} to S3 at {destinationZipKey}");
        }
    }
}
