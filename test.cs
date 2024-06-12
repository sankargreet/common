using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Newtonsoft.Json;

class Program
{
    static async Task Main(string[] args)
    {
        string bucketName = "your-bucket-name";
        string objectKey = "your-object-key";
        string region = "us-west-2"; // Specify your region
        string awsAccessKeyId = "your-access-key-id";
        string awsSecretAccessKey = "your-secret-access-key";
        int urlValidityDurationInMinutes = 60; // Duration for which the URL is valid

        var s3Client = new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey, Amazon.RegionEndpoint.GetBySystemName(region));

        // Simulate receiving file content from a producer
        byte[] fileContent = Encoding.UTF8.GetBytes("This is the content of the file.");

        var postResponse = GeneratePreSignedPost(s3Client, bucketName, objectKey, urlValidityDurationInMinutes, fileContent);

        Console.WriteLine("Pre-Signed POST URL: " + postResponse.Url);
        foreach (var field in postResponse.Fields)
        {
            Console.WriteLine($"{field.Key}: {field.Value}");
        }

        await UploadFileToS3Async(postResponse, fileContent);
    }

    static PostResponse GeneratePreSignedPost(IAmazonS3 s3Client, string bucketName, string objectKey, int validityDurationInMinutes, byte[] fileContent)
    {
        var expiration = DateTime.UtcNow.AddMinutes(validityDurationInMinutes).ToString("yyyy-MM-ddTHH:mm:ssZ");
        var policy = new Dictionary<string, object>
        {
            { "expiration", expiration },
            { "conditions", new List<object>
                {
                    new Dictionary<string, string> { { "bucket", bucketName } },
                    new Dictionary<string, string> { { "key", objectKey } },
                    new Dictionary<string, string> { { "acl", "private" } },
                    new Dictionary<string, string> { { "x-amz-algorithm", "AWS4-HMAC-SHA256" } },
                    new Dictionary<string, string> { { "x-amz-credential", $"{s3Client.Config.AWSAccessKeyId}/{DateTime.UtcNow:yyyyMMdd}/{s3Client.Config.RegionEndpoint.SystemName}/s3/aws4_request" } },
                    new Dictionary<string, string> { { "x-amz-date", DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ") } },
                    new Dictionary<string, string> { { "content-type", "text/plain" } }
                }
            }
        };

        string policyString = JsonConvert.SerializeObject(policy);
        string base64Policy = Convert.ToBase64String(Encoding.UTF8.GetBytes(policyString));

        // Calculate the signing key
        string dateKey = HmacSHA256(DateTime.UtcNow.ToString("yyyyMMdd"), "AWS4" + s3Client.Config.AWSSecretAccessKey);
        string dateRegionKey = HmacSHA256(s3Client.Config.RegionEndpoint.SystemName, dateKey);
        string dateRegionServiceKey = HmacSHA256("s3", dateRegionKey);
        string signingKey = HmacSHA256("aws4_request", dateRegionServiceKey);

        // Sign the policy
        string signature = HmacSHA256(base64Policy, signingKey);

        var response = new PostResponse
        {
            Url = $"https://{bucketName}.s3.amazonaws.com/",
            Fields = new Dictionary<string, string>
            {
                { "key", objectKey },
                { "acl", "private" },
                { "policy", base64Policy },
                { "x-amz-algorithm", "AWS4-HMAC-SHA256" },
                { "x-amz-credential", $"{s3Client.Config.AWSAccessKeyId}/{DateTime.UtcNow:yyyyMMdd}/{s3Client.Config.RegionEndpoint.SystemName}/s3/aws4_request" },
                { "x-amz-date", DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ") },
                { "x-amz-signature", signature }
            }
        };

        // Compute Content-MD5
        var md5Hash = ComputeMD5(fileContent);
        response.Fields.Add("Content-MD5", md5Hash);

        return response;
    }

    static string ComputeMD5(byte[] content)
    {
        using (var md5 = MD5.Create())
        {
            var hash = md5.ComputeHash(content);
            return Convert.ToBase64String(hash);
        }
    }

    static async Task UploadFileToS3Async(PostResponse postResponse, byte[] fileContent)
    {
        using (var client = new HttpClient())
        {
            using (var content = new MultipartFormDataContent())
            {
                foreach (var field in postResponse.Fields)
                {
                    content.Add(new StringContent(field.Value), field.Key);
                }

                var fileContentContent = new ByteArrayContent(fileContent);
                fileContentContent.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
                fileContentContent.Headers.ContentMD5 = Convert.FromBase64String(postResponse.Fields["Content-MD5"]);

                content.Add(fileContentContent, "file");

                var response = await client.PostAsync(postResponse.Url, content);
                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine("File uploaded successfully.");
                }
                else
                {
                    Console.WriteLine("File upload failed. Status code: " + response.StatusCode);
                }
            }
        }
    }

    static string HmacSHA256(string data, string key)
    {
        using (var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(key)))
        {
            var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(data));
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }
    }
}

public class PostResponse
{
    public string Url { get; set; }
    public IDictionary<string, string> Fields { get; set; }
}
