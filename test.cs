
                // Step 3: Copy the entry to a MemoryStream
                using (MemoryStream entryStream = new MemoryStream())
                {
                    using (Stream originalEntryStream = entry.Open())
                    {
                        await originalEntryStream.CopyToAsync(entryStream);
                    }
                    entryStream.Position = 0; // Reset stream position

                    // Define the destination path in S3
                    string s3Key = destinationFolder + entry.FullName;

                    var uploadRequest = new PutObjectRequest
                    {
                        BucketName = bucketName,
                        Key = s3Key,
                        InputStream = entryStream
                    };

                    // Upload each file extracted from the zip archive to S3
                    await s3Client.PutObjectAsync(uploadRequest);
                    Console.WriteLine($"Uploaded {entry.FullName} to S3 at {s3Key}");
                }
