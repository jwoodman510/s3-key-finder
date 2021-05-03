using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using CsvHelper;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace s3_size_finder
{
    public class Finder : IDisposable
    {
        private readonly IAmazonS3 _s3Client;
        private readonly AppSettings _appSettings;

        public Finder(AppSettings appSettings)
        {
            _appSettings = appSettings;
            _s3Client = BuildS3Client();
        }

        public void Dispose()
        {
            _s3Client.Dispose();
        }

        /// <summary>
        /// Finds files in s3 matching the application settings criteria
        /// </summary>
        /// <returns>TJe result set file path.</returns>
        public async Task<string> FindAsync()
        {
            Consoler.WriteLines(ConsoleColor.Cyan, $"Finding files between {0} and {0} bytes...");

            string continuationToken = null;
            var keyMatches = new ConcurrentDictionary<string, long>();

            var retryAttempts = 3;

            do
            {
                try
                {
                    Consoler.WriteLines(ConsoleColor.White, "Fetching object list.");

                    if (!string.IsNullOrEmpty(continuationToken))
                    {
                        Consoler.WriteLines(ConsoleColor.White, $"ContinuationToken={continuationToken}");
                    }

                    var response = await _s3Client.ListObjectsV2Async(new Amazon.S3.Model.ListObjectsV2Request
                    {
                        BucketName = _appSettings.BucketName,
                        ContinuationToken = continuationToken
                    });

                    Consoler.WriteLines(ConsoleColor.White, $"Found {response.S3Objects?.Count} objects.");

                    continuationToken = response.IsTruncated ? response.NextContinuationToken : null;

                    var matches = response.S3Objects
                        .AsParallel()
                        .Where(x => IsMatch(x))
                        .Select(x => new
                        {
                            x.Key,
                            x.Size
                        })
                        .Distinct();

                    var numMatches = 0;

                    foreach (var match in matches)
                    {
                        numMatches++;

                        keyMatches[match.Key] = match.Size;
                    }

                    Consoler.WriteLines(ConsoleColor.Cyan, $"Found {numMatches} objects matching size criteria.");
                }
                catch (Exception ex)
                {
                    if (retryAttempts == 0)
                    {
                        Consoler.WriteError("0 retry attempts remaining. Aborting.");

                        continuationToken = null;
                    }
                    else
                    {
                        Consoler.WriteError($"Error occurred while fetching object list: {ex.Message}. {retryAttempts} retries remaining.");

                        var delayInSeconds = retryAttempts * 5;

                        Consoler.WriteLines(ConsoleColor.Yellow, $"Delaying for {delayInSeconds} seconds before next attempt.");

                        await Task.Delay(TimeSpan.FromSeconds(delayInSeconds));

                        retryAttempts--;
                    }
                }
            }
            while (!string.IsNullOrEmpty(continuationToken));

            return await WriteCsvAsync(keyMatches);
        }

        private bool IsMatch(Amazon.S3.Model.S3Object s3Object)
        {
            if (_appSettings.MinSizeBytes >= 0 && s3Object.Size < _appSettings.MinSizeBytes)
            {
                return false;
            }

            if (_appSettings.MaxSizeBytes >= 0 && s3Object.Size > _appSettings.MaxSizeBytes)
            {
                return false;
            }

            if (!string.IsNullOrEmpty(_appSettings.KeyPattern) && !Regex.IsMatch(s3Object.Key, _appSettings.KeyPattern))
            {
                return false;
            }

            return true;
        }

        private static string GetBasePath()
        {
            using var processModule = Process.GetCurrentProcess().MainModule;

            return Path.GetDirectoryName(processModule?.FileName);
        }

        private async Task<string> WriteCsvAsync(IDictionary<string, long> keyMatches)
        {
            Consoler.WriteLines(ConsoleColor.Cyan, "Writing results to CSV...");

            var filePath = Path.Combine(GetBasePath(), $"{Guid.NewGuid()}.csv");

            using var fileStream = new FileStream(filePath, FileMode.CreateNew);
            using var streamWriter = new StreamWriter(fileStream, Encoding.UTF8, 1024, leaveOpen: true);
            using var csvWriter = new CsvWriter(streamWriter, CultureInfo.InvariantCulture);

            csvWriter.WriteField("key");
            csvWriter.WriteField("size");

            await csvWriter.NextRecordAsync();

            foreach (var record in keyMatches)
            {
                csvWriter.WriteField(record.Key);
                csvWriter.WriteField(record.Value);

                await csvWriter.NextRecordAsync();
            }

            return filePath;
        }

        private IAmazonS3 BuildS3Client()
        {
            var credentials = new BasicAWSCredentials(_appSettings.AccessKey, _appSettings.SecretKey);
            var regionEndpoint = RegionEndpoint.GetBySystemName(_appSettings.Region);

            return new AmazonS3Client(credentials, regionEndpoint);
        }
    }
}
