using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

class S3ZipFileDownloader
{
    private static readonly string bucketName = "your-bucket-name";
    private static readonly string keyName = "your-zip-file.zip";
    private static readonly string localFilePath = "/tmp/your-zip-file.zip"; // Local path in the pod
    private static readonly string extractPath = "/tmp/unzipped-files"; // Path where files will be extracted

    private static readonly AmazonS3Client s3Client = new AmazonS3Client();

    public static async Task Main(string[] args)
    {
        try
        {
            // Step 1: Download the ZIP file from S3
            Console.WriteLine("Downloading the ZIP file from S3...");
            await DownloadZipFileFromS3();

            // Step 2: Unzip the file
            Console.WriteLine("Unzipping the file...");
            UnzipFile(localFilePath, extractPath);

            Console.WriteLine("File unzipped successfully.");
        }
        catch (AmazonS3Exception e)
        {
            Console.WriteLine($"Error downloading the file from S3: {e.Message}");
        }
        catch (Exception e)
        {
            Console.WriteLine($"An error occurred: {e.Message}");
        }
    }

    private static async Task DownloadZipFileFromS3()
    {
        using (GetObjectResponse response = await s3Client.GetObjectAsync(bucketName, keyName))
        using (Stream responseStream = response.ResponseStream)
        using (FileStream fileStream = new FileStream(localFilePath, FileMode.Create, FileAccess.Write))
        {
            await responseStream.CopyToAsync(fileStream);
        }
        Console.WriteLine($"File downloaded to {localFilePath}");
    }

    private static void UnzipFile(string zipFilePath, string destinationPath)
    {
        if (Directory.Exists(destinationPath))
        {
            Directory.Delete(destinationPath, true); // Clean up if directory already exists
        }

        Directory.CreateDirectory(destinationPath);
        ZipFile.ExtractToDirectory(zipFilePath, destinationPath);
        Console.WriteLine($"Files extracted to {destinationPath}");
    }
}
