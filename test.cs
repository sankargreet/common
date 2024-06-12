using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;

class Program
{
    static async Task Main(string[] args)
    {
        string bucketName = "your-bucket-name";
        string objectKeyPrefix = "your-object-key-prefix";
        string region = "us-west-2"; // Specify your region
        int urlValidityDurationInMinutes = 60; // Duration for which the URL is valid

        var s3Client = new AmazonS3Client(Amazon.RegionEndpoint.GetBySystemName(region));

        // Simulate receiving file content from a producer
        byte[] fileContent = Encoding.UTF8.GetBytes("This is the content of the file.");
        
        var postResponse = GeneratePreSignedPost(s3Client, bucketName, objectKeyPrefix, urlValidityDurationInMinutes, fileContent);

        Console.WriteLine("Pre-Signed POST URL: " + postResponse.Url);
        foreach (var field in postResponse.Fields)
        {
            Console.WriteLine($"{field.Key}: {field.Value}");
        }
    }

    static PostResponse GeneratePreSignedPost(IAmazonS3 s3Client, string bucketName, string objectKeyPrefix, int validityDurationInMinutes, byte[] fileContent)
    {
        // Create the policy conditions
        var conditions = new List<PostPolicyCondition>
        {
            new PostPolicyCondition
            {
                Type = PostPolicyConditionType.StartsWith,
                Condition = new[] { "key", objectKeyPrefix }
            },
            new PostPolicyCondition
            {
                Type = PostPolicyConditionType.ContentType,
                Condition = new[] { "text/plain" }
            },
            new PostPolicyCondition
            {
                Type = PostPolicyConditionType.ContentType,
                Condition = new[] { "application/zip" }
            },
            new PostPolicyCondition
            {
                Type = PostPolicyConditionType.ContentType,
                Condition = new[] { "text/csv" }
            },
            new PostPolicyCondition
            {
                Type = PostPolicyConditionType.Acl,
                Condition = new[] { "private" }
            },
            new PostPolicyCondition
            {
                Type = PostPolicyConditionType.ContentLengthRange,
                Condition = new[] { "0", "10485760" } // Max file size 10 MB
            },
            new PostPolicyCondition
            {
                Type = PostPolicyConditionType.XAmzAlgorithm,
                Condition = new[] { "AWS4-HMAC-SHA256" }
            }
        };

        var expiration = DateTime.UtcNow.AddMinutes(validityDurationInMinutes);

        // Create the post policy
        var policy = new S3PostPolicy
        {
            Expiration = expiration,
            Conditions = conditions
        };

        // Generate the pre-signed POST URL
        var postUrl = AmazonS3Util.GeneratePostPreSignedUrl(s3Client, bucketName, objectKeyPrefix, expiration, policy);

        // Compute Content-MD5
        var md5Hash = ComputeMD5(fileContent);

        var response = new PostResponse
        {
            Url = postUrl.Uri.ToString(),
            Fields = postUrl.Fields
        };

        // Add Content-MD5 to the fields
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
}

public class PostResponse
{
    public string Url { get; set; }
    public IDictionary<string, string> Fields { get; set; }
}
