 using (var zipStream = new ZipArchive(Response.BodyWriter.AsStream(), ZipArchiveMode.Create, true))
        {
            foreach (var key in fileKeys)
            {
                var request = new GetObjectRequest { BucketName = _bucketName, Key = key };

                using (var response = await _s3Client.GetObjectAsync(request))
                using (var entryStream = zipStream.CreateEntry(Path.GetFileName(key)).Open())
                {
                    await DownloadFileInParts(response.ResponseStream, entryStream);
                }
            }
        }

        return new EmptyResult();
    }

    private async Task DownloadFileInParts(Stream sourceStream, Stream destinationStream, int partSizeMB = 5)
    {
        byte[] buffer = new byte[partSizeMB * 1024 * 1024]; // Multipart size in MB
        int bytesRead;

        while ((bytesRead = await sourceStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
        {
            await destinationStream.WriteAsync(buffer, 0, bytesRead);
        }
    }
