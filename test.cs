using System;
using System.IO;
using System.IO.Compression;
using Amazon.S3;
using Amazon.S3.Transfer;

public class Program
{
    private static readonly string directoryPath = "/tmp/files";
    private static readonly string zipFilePath = "/tmp/files.zip";
    private static readonly string bucketName = "your-bucket-name";
    private static readonly string keyName = "path/to/your/zipfile.zip";

    private static readonly AmazonS3Client s3Client = new AmazonS3Client(Amazon.RegionEndpoint.USEast1);

    public static void Main(string[] args)
    {
        try
        {
            // Step 1: Create files
            FileCreator.CreateFiles(directoryPath);

            // Step 2: Zip the created files
            ZipFiles.CreateZipFile(directoryPath, zipFilePath);

            // Step 3: Upload the zip file to S3
            S3Uploader.UploadFile(zipFilePath, bucketName, keyName);

            // Step 4: Delete the folder
            Directory.Delete(directoryPath, true);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }
    }
}

public class FileCreator
{
    public static void CreateFiles(string directoryPath)
    {
        Directory.CreateDirectory(directoryPath);

        for (int i = 1; i <= 5; i++)
        {
            string filePath = Path.Combine(directoryPath, $"file{i}.txt");
            File.WriteAllText(filePath, $"This is the content of file{i}.");
        }
    }
}

public class ZipFiles
{
    public static void CreateZipFile(string sourceDirectory, string zipFilePath)
    {
        if (File.Exists(zipFilePath))
        {
            File.Delete(zipFilePath);
        }

        ZipFile.CreateFromDirectory(sourceDirectory, zipFilePath);
    }
}

public class S3Uploader
{
    public static void UploadFile(string filePath, string bucketName, string keyName)
    {
        try
        {
            var fileTransferUtility = new TransferUtility(s3Client);
            fileTransferUtility.Upload(filePath, bucketName, keyName);
            Console.WriteLine("Upload completed.");
        }
        catch (AmazonS3Exception e)
        {
            Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
        }
        catch (Exception e)
        {
            Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
        }
    }
}
