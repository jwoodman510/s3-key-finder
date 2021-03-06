using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

        public async Task<string> FindAsync()
        {
            string filePath = null;
            IDictionary<string, long> keyMatches;

            if (!string.IsNullOrEmpty(_appSettings.SourceDataFilePath))
            {
                keyMatches = await FindViaFileAsync();
            }
            else
            {
                keyMatches = await FindViaS3Async();

                filePath = await WriteCsvAsync(keyMatches);
            }

            if (keyMatches.Count > 0)
            {
                await InvokeActionAsync(keyMatches.Keys);
            }

            return filePath;
        }

        public async Task<ConcurrentDictionary<string, long>> FindViaFileAsync()
        {
            using var reader = new StreamReader(_appSettings.SourceDataFilePath);
            using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                MissingFieldFound = x => { }
            });

            var keys = new ConcurrentDictionary<string, long>();

            var records = csv.GetRecordsAsync<KeyRecord>();

            await foreach (var record in records)
            {
                keys[record.Key] = record.Size ?? -1;
            }

            return keys;
        }

        public async Task<ConcurrentDictionary<string, long>> FindViaS3Async()
        {
            Consoler.WriteLines(ConsoleColor.Cyan,
                "File Key Filter Criteria:",
                $"\tSize between {0} and {0} bytes",
                $"\tKey pattern: {_appSettings.KeyPattern}");

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

                        keyMatches.TryAdd(match.Key, match.Size);
                    }

                    Consoler.WriteLines(ConsoleColor.Cyan, $"Found {numMatches} objects matching filter criteria.");
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

            return keyMatches;
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

        private async Task<string> WriteCsvAsync(IDictionary<string, long> keyMatches)
        {
            Consoler.WriteLines(ConsoleColor.Cyan, "Writing results to CSV...");

            var filePath = Path.Combine(AppSettings.BasePath, $"{AppSettings.OpId}_find.csv");

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

        private async Task InvokeActionAsync(IEnumerable<string> keys)
        {
            if (string.IsNullOrEmpty(_appSettings.Action?.Name))
            {
                return;
            }

            Consoler.WriteLines(ConsoleColor.Yellow, $"Invoking Action {_appSettings.Action?.Name}.");

            switch (_appSettings.Action.Name)
            {
                case DeleteAction.Name:
                    await DeleteAction.InvokeAsync(keys, _s3Client, _appSettings);
                    break;
                case RenameAction.Name:
                    await RenameAction.InvokeAsync(keys, _s3Client, _appSettings);
                    break;
                default:
                    Consoler.WriteError($"Action Not Supported: {_appSettings.Action.Name}");
                    break;
            }
        }
    }
}
