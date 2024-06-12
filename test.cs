using System;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

class Program
{
    static void Main(string[] args)
    {
        string bucketName = "your-bucket-name";
        string objectKey = $"uploads/{DateTime.UtcNow:yyyy/MM/dd}/my-file-{DateTime.UtcNow:yyyyMMddHHmmss}.txt";
        string region = "us-west-2"; // Specify your region
        string awsAccessKeyId = "your-access-key-id";
        string awsSecretAccessKey = "your-secret-access-key";
        string vpcEndpointUrl = "https://your-vpc-endpoint-url"; // Your VPC endpoint URL

        var s3Config = new AmazonS3Config
        {
            ServiceURL = vpcEndpointUrl, // Use the VPC endpoint URL
            RegionEndpoint = RegionEndpoint.GetBySystemName(region)
        };

        var s3Client = new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey, s3Config);

        // Generate the pre-signed URL
        string preSignedUrl = GeneratePreSignedURL(s3Client, bucketName, objectKey, 60);

        Console.WriteLine("Pre-Signed URL: " + preSignedUrl);
    }

    static string GeneratePreSignedURL(IAmazonS3 s3Client, string bucketName, string objectKey, int validityDurationInMinutes)
    {
        var request = new GetPreSignedUrlRequest
        {
            BucketName = bucketName,
            Key = objectKey,
            Expires = DateTime.UtcNow.AddMinutes(validityDurationInMinutes),
            Verb = HttpVerb.PUT,
            ContentType = "text/plain"
        };

        string url = s3Client.GetPreSignedURL(request);
        return url;
    }
}



using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        // Pre-signed URL generated from the previous step
        string preSignedUrl = "https://your-pre-signed-url";
        
        // Simulate receiving file content from a producer
        byte[] fileContent = Encoding.UTF8.GetBytes("This is the content of the file.");

        // Calculate Content-MD5 hash
        string contentMD5 = ComputeMD5(fileContent);

        // Validate MD5 hash from producer
        bool md5IsValid = ValidateMD5FromProducer(contentMD5);

        if (md5IsValid)
        {
            await UploadFileToS3Async(preSignedUrl, fileContent, contentMD5);
        }
        else
        {
            Console.WriteLine("MD5 hash from producer does not match.");
            // Handle validation failure
        }
    }

    static async Task UploadFileToS3Async(string preSignedUrl, byte[] fileContent, string contentMD5)
    {
        using (var client = new HttpClient())
        {
            using (var content = new ByteArrayContent(fileContent))
            {
                content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
                content.Headers.ContentMD5 = Convert.FromBase64String(contentMD5);
                content.Headers.Add("x-amz-algorithm", "AWS4-HMAC-SHA256");

                var request = new HttpRequestMessage(HttpMethod.Put, preSignedUrl)
                {
                    Content = content
                };

                var response = await client.SendAsync(request);
                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine("File uploaded successfully.");
                }
                else
                {
                    Console.WriteLine("File upload failed. Status code: " + response.StatusCode);
                    var responseBody = await response.Content.ReadAsStringAsync();
                    Console.WriteLine("Response body: " + responseBody);
                }
            }
        }
    }

    static string ComputeMD5(byte[] content)
    {
        using (var md5 = MD5.Create())
        {
            var hash = md5.ComputeHash(content);
            return Convert.ToBase64String(hash);
        }
    }

    static bool ValidateMD5FromProducer(string computedMD5)
    {
        // Example: Retrieve MD5 hash from producer (simulated)
        string md5FromProducer = "eB5eJF1ptWaXm4bijSPyxw=="; // Replace with actual MD5 hash received

        // Compare computed MD5 hash with MD5 hash from producer
        return string.Equals(computedMD5, md5FromProducer);
    }
}
