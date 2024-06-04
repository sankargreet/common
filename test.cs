using Amazon.S3;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

class Program
{
    private static readonly IAmazonS3 _s3Client; // Replace with your S3 client instance
    private static readonly string _bucketName = "your-bucket-name"; // Replace with your bucket name
    private static readonly string _objectKey = "your-object-key"; // Replace with your object key
    private static readonly long _chunkSize = 1024; // Adjust chunk size as needed
    private static readonly string _downloadFolder = "downloaded_chunks";

    static async Task Main(string[] args)
    {
        Console.WriteLine("Downloading S3 object in chunks...");

        try
        {
            // Ensure download folder exists
            Directory.CreateDirectory(_downloadFolder);

            var downloader = new MultiPartDownloader(_s3Client, _bucketName, _objectKey, _chunkSize, _downloadFolder);
            await downloader.Download();

            Console.WriteLine("Download complete!");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(<span class="math-inline">"Error downloading object\: \{ex\.Message\}"\);
\}
\}
\}
public class MultiPartDownloader
\{
private readonly IAmazonS3 \_s3Client;
private readonly string \_bucketName;
private readonly string \_objectKey;
private readonly long \_chunkSize;
private readonly string \_downloadFolder;
public MultiPartDownloader\(IAmazonS3 s3Client, string bucketName, string objectKey, long chunkSize, string downloadFolder\)
\{
\_s3Client \= s3Client;
\_bucketName \= bucketName;
\_objectKey \= objectKey;
\_chunkSize \= chunkSize;
\_downloadFolder \= downloadFolder;
\}
public async Task Download\(\)
\{
GetObjectRequest request \= new GetObjectRequest
\{
BucketName \= \_bucketName,
Key \= \_objectKey
\};
using \(GetObjectResponse response \= await \_s3Client\.GetObjectAsync\(request\)\)
\{
using \(Stream responseStream \= response\.ResponseStream\)
\{
using \(ChunkReader reader \= new ChunkReader\(responseStream, \_chunkSize\)\)
\{
int chunkNumber \= 1;
string chunk;
while \(\(chunk \= await reader\.ReadNextChunk\(\)\) \!\= null\)
\{
await SaveChunkToFile\(chunk, chunkNumber\);
Console\.WriteLine\(</span>"Downloaded chunk {chunkNumber}");
                        chunkNumber++;
                    }
                }
            }
        }
    }

    private async Task SaveChunkToFile(string chunk, int chunkNumber)
    {
        string fileName = Path.Combine(_downloadFolder, $"{_objectKey}-chunk-{chunkNumber}.txt"); // Adjust file extension and naming convention as needed
        using (StreamWriter writer = new StreamWriter(fileName, false))
        {
            await writer.WriteAsync(chunk);
        }
    }
}

public class ChunkReader
{
    private readonly Stream _stream;
    private readonly long _chunkSize;
    private readonly byte[] _buffer;

    public ChunkReader(Stream stream, long chunkSize)
    {
        _stream = stream;
        _chunkSize = chunkSize;
        _buffer = new byte[chunkSize];
    }

    public async Task<string> ReadNextChunk()
    {
        long bytesToRead = _chunkSize;

        while (_stream.CanRead && bytesToRead > 0)
        {
            int bytesRead = await _stream.ReadAsync(_buffer, 0, (int)Math.Min(bytesToRead, _buffer.Length));
            if (bytesRead == 0) // End of stream
            {
                break;
            }

            string chunkData = Encoding.UTF8.GetString(_buffer, 0, bytesRead); // Adjust encoding based on your data
            int newlinePos = chunkData.IndexOf('\n');

            if (newlinePos >= 0)
            {
                string chunk = chunkData.Substring(0, newlinePos + 1);
                return chunk;
            }

            bytesToRead -= bytesRead;
        }

        // Handle remaining data in the buffer if not a complete line
        if (_stream.Position < _
