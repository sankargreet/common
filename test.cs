using Amazon.S3;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

public class MultiPartDownloader
{
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;
    private readonly string _objectKey;
    private readonly long _chunkSize;
    private readonly string _downloadFolder;

    public MultiPartDownloader(IAmazonS3 s3Client, string bucketName, string objectKey, long chunkSize, string downloadFolder)
    {
        _s3Client = s3Client;
        _bucketName = bucketName;
        _objectKey = objectKey;
        _chunkSize = chunkSize;
        _downloadFolder = downloadFolder;

        // Ensure the download folder exists
        Directory.CreateDirectory(_downloadFolder);
    }

    public async Task Download()
    {
        GetObjectRequest request = new GetObjectRequest
        {
            BucketName = _bucketName,
            Key = _objectKey
        };

        using (GetObjectResponse response = await _s3Client.GetObjectAsync(request))
        {
            using (Stream responseStream = response.ResponseStream)
            {
                using (ChunkReader reader = new ChunkReader(responseStream, _chunkSize))
                {
                    int chunkNumber = 1;
                    string chunk;
                    while ((chunk = await reader.ReadNextChunk()) != null)
                    {
                        await SaveChunkToFile(chunk, chunkNumber);
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

    private async Task ProcessChunk(string chunk)
    {
        // Optional: You can still implement in-memory processing logic here if needed
        // ...
    }
}

public class ChunkReader
{
    private readonly Stream _stream;
    private readonly long _chunkSize;
    private readonly StringBuilder _buffer;

    public ChunkReader(Stream stream, long chunkSize)
    {
        _stream = stream;
        _chunkSize = chunkSize;
        _buffer = new StringBuilder();
    }

    public async Task<string> ReadNextChunk()
    {
        long bytesToRead = _chunkSize;

        while (_stream.CanRead && bytesToRead > 0)
        {
            int nextByte = await _stream.PeekAsync();
            if (nextByte == -1) // End of stream
            {
                break;
            }

            char nextChar = (char)nextByte;
            _buffer.Append(nextChar);

            if (nextChar == '\n') // Complete line encountered
            {
                string chunk = _buffer.ToString();
                _buffer.Clear();
                return chunk;
            }

            await _stream.ReadByteAsync(); // Consume the byte after peeking
            bytesToRead--;
        }

        // Handle remaining data in the buffer if not a complete line
        if (_buffer.Length > 0)
        {
            string remainingChunk = _buffer.ToString();
            _buffer.Clear();
            return remainingChunk;
        }

        return null; // No more data available
    }
}
