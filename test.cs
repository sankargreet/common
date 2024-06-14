using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json;

class PreSignedUrlGenerator
{
    public static void Main(string[] args)
    {
        string bucketName = "your-bucket-name";
        string region = "us-west-2"; // Specify your region
        string awsAccessKeyId = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
        string awsSecretAccessKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");
        string fileName = "example.txt"; // The filename you want to include as metadata
        string vpcEndpointUrl = "https://your-vpc-endpoint-url"; // Your VPC endpoint URL

        var formData = GenerateS3PostPolicy(bucketName, region, awsAccessKeyId, awsSecretAccessKey, fileName, vpcEndpointUrl);
        
        Console.WriteLine("Form Data:");
        foreach (var kvp in formData)
        {
            Console.WriteLine($"{kvp.Key}: {kvp.Value}");
        }
    }

    static Dictionary<string, string> GenerateS3PostPolicy(string bucketName, string region, string awsAccessKeyId, string awsSecretAccessKey, string fileName, string vpcEndpointUrl)
    {
        DateTime expiration = DateTime.UtcNow.AddHours(1); // Set expiration time for the policy
        string policy = JsonConvert.SerializeObject(new
        {
            expiration = expiration.ToString("yyyy-MM-ddTHH:mm:ssZ"),
            conditions = new object[]
            {
                new Dictionary<string, string> { { "bucket", bucketName } },
                new Dictionary<string, string> { { "key", fileName } },
                new Dictionary<string, string> { { "acl", "public-read" } },
                new Dictionary<string, string> { { "x-amz-meta-filename", fileName } },
                new[] { "starts-with", "$Content-Type", "" },
                new[] { "content-length-range", 0, 5242880000 }, // 5GB
                new Dictionary<string, string> { { "x-amz-credential", $"{awsAccessKeyId}/{DateTime.UtcNow:yyyyMMdd}/{region}/s3/aws4_request" } },
                new Dictionary<string, string> { { "x-amz-algorithm", "AWS4-HMAC-SHA256" } },
                new Dictionary<string, string> { { "x-amz-date", DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ") } }
            }
        });

        string base64Policy = Convert.ToBase64String(Encoding.UTF8.GetBytes(policy));
        string signingKey = GetSigningKey(awsSecretAccessKey, DateTime.UtcNow.ToString("yyyyMMdd"), region, "s3");
        string signature = HexEncodeHash(signingKey, base64Policy);

        var formData = new Dictionary<string, string>
        {
            { "url", vpcEndpointUrl },
            { "key", fileName },
            { "acl", "public-read" },
            { "x-amz-meta-filename", fileName },
            { "Content-Type", "text/plain" },
            { "x-amz-credential", $"{awsAccessKeyId}/{DateTime.UtcNow:yyyyMMdd}/{region}/s3/aws4_request" },
            { "x-amz-algorithm", "AWS4-HMAC-SHA256" },
            { "x-amz-date", DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ") },
            { "Policy", base64Policy },
            { "x-amz-signature", signature }
        };

        return formData;
    }

    static string GetSigningKey(string secretKey, string dateStamp, string regionName, string serviceName)
    {
        byte[] kDate = Sign(Encoding.UTF8.GetBytes("AWS4" + secretKey), dateStamp);
        byte[] kRegion = Sign(kDate, regionName);
        byte[] kService = Sign(kRegion, serviceName);
        byte[] kSigning = Sign(kService, "aws4_request");
        return Convert.ToBase64String(kSigning);
    }

    static byte[] Sign(byte[] key, string message)
    {
        using (var hmac = new HMACSHA256(key))
        {
            return hmac.ComputeHash(Encoding.UTF8.GetBytes(message));
        }
    }

    static string HexEncodeHash(string key, string message)
    {
        byte[] keyBytes = Convert.FromBase64String(key);
        using (var hmac = new HMACSHA256(keyBytes))
        {
            return BitConverter.ToString(hmac.ComputeHash(Encoding.UTF8.GetBytes(message))).Replace("-", "").ToLower();
        }
    }
}












using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json;

class PreSignedUrlGenerator
{
    public static void Main(string[] args)
    {
        string bucketName = "your-bucket-name";
        string region = "us-west-2"; // Specify your region
        string awsAccessKeyId = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
        string awsSecretAccessKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");
        string fileName = "example.txt"; // The filename you want to include as metadata
        string vpcEndpointUrl = "https://your-vpc-endpoint-url"; // Your VPC endpoint URL

        var formData = GenerateS3PostPolicy(bucketName, region, awsAccessKeyId, awsSecretAccessKey, fileName, vpcEndpointUrl);
        
        Console.WriteLine("Form Data:");
        foreach (var kvp in formData)
        {
            Console.WriteLine($"{kvp.Key}: {kvp.Value}");
        }
    }

    static Dictionary<string, string> GenerateS3PostPolicy(string bucketName, string region, string awsAccessKeyId, string awsSecretAccessKey, string fileName, string vpcEndpointUrl)
    {
        DateTime expiration = DateTime.UtcNow.AddHours(1); // Set expiration time for the policy
        string policy = JsonConvert.SerializeObject(new
        {
            expiration = expiration.ToString("yyyy-MM-ddTHH:mm:ssZ"),
            conditions = new object[]
            {
                new Dictionary<string, string> { { "bucket", bucketName } },
                new Dictionary<string, string> { { "key", fileName } },
                new Dictionary<string, string> { { "acl", "public-read" } },
                new Dictionary<string, string> { { "x-amz-meta-filename", fileName } },
                new[] { "starts-with", "$Content-Type", "" },
                new[] { "content-length-range", 0, 5242880000 }, // 5GB
                new Dictionary<string, string> { { "x-amz-credential", $"{awsAccessKeyId}/{DateTime.UtcNow:yyyyMMdd}/{region}/s3/aws4_request" } },
                new Dictionary<string, string> { { "x-amz-algorithm", "AWS4-HMAC-SHA256" } },
                new Dictionary<string, string> { { "x-amz-date", DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ") } }
            }
        });

        string base64Policy = Convert.ToBase64String(Encoding.UTF8.GetBytes(policy));
        string signingKey = GetSigningKey(awsSecretAccessKey, DateTime.UtcNow.ToString("yyyyMMdd"), region, "s3");
        string signature = HexEncodeHash(signingKey, base64Policy);

        var formData = new Dictionary<string, string>
        {
            { "url", vpcEndpointUrl },
            { "key", fileName },
            { "acl", "public-read" },
            { "x-amz-meta-filename", fileName },
            { "Content-Type", "text/plain" },
            { "x-amz-credential", $"{awsAccessKeyId}/{DateTime.UtcNow:yyyyMMdd}/{region}/s3/aws4_request" },
            { "x-amz-algorithm", "AWS4-HMAC-SHA256" },
            { "x-amz-date", DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ") },
            { "Policy", base64Policy },
            { "x-amz-signature", signature }
        };

        return formData;
    }

    static string GetSigningKey(string secretKey, string dateStamp, string regionName, string serviceName)
    {
        byte[] kDate = Sign(Encoding.UTF8.GetBytes("AWS4" + secretKey), dateStamp);
        byte[] kRegion = Sign(kDate, regionName);
        byte[] kService = Sign(kRegion, serviceName);
        byte[] kSigning = Sign(kService, "aws4_request");
        return Convert.ToBase64String(kSigning);
    }

    static byte[] Sign(byte[] key, string message)
    {
        using (var hmac = new HMACSHA256(key))
        {
            return hmac.ComputeHash(Encoding.UTF8.GetBytes(message));
        }
    }

    static string HexEncodeHash(string key, string message)
    {
        byte[] keyBytes = Convert.FromBase64String(key);
        using (var hmac = new HMACSHA256(keyBytes))
        {
            return BitConverter.ToString(hmac.ComputeHash(Encoding.UTF8.GetBytes(message))).Replace("-", "").ToLower();
        }
    }
}
