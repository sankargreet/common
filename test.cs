 foreach (var key in fileKeys)
            {
                var request = new GetObjectRequest { BucketName = _bucketName, Key = key };

                using (var response = await _s3Client.GetObjectAsync(request))
                using (var s3Stream = response.ResponseStream)
                {
                    // Check if the file is already a ZIP
                    if (Path.GetExtension(key).Equals(".zip", StringComparison.OrdinalIgnoreCase))
                    {
                        // Copy the ZIP file directly without unzipping
                        var zipEntry = zipStream.CreateEntry(Path.GetFileName(key));
                        using (var entryStream = zipEntry.Open())
                        {
                            await s3Stream.CopyToAsync(entryStream);
                        }
                    }
                    else
                    {
                        // Add non-ZIP files normally
                        var entry = zipStream.CreateEntry(Path.GetFileName(key));
                        using (var entryStream = entry.Open())
                        {
                            await DownloadFileInParts(s3Stream, entryStream);
                        }
                    }
                }
            }
