using Amazon.S3;
using Amazon.S3.Model;
using CsvHelper;
using MoreLinq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace s3_size_finder
{
    public class RenameAction
    {
        public const string Name = "RENAME";

        public static async Task InvokeAsync(IEnumerable<string> keys, IAmazonS3 s3Client, AppSettings appSettings)
        {
            var batchNumber = 1;
            var batchSize = appSettings.Action.BatchSize > 0 ? appSettings.Action.BatchSize : 100;

            var copies = new ConcurrentDictionary<string, string>();

            foreach (var batch in keys.Batch(batchSize))
            {
                Consoler.WriteLines(ConsoleColor.White, $"Renaming Batch: {batchNumber}");

                if (appSettings.Action.DryRun)
                {
                    batch.Select(x => (source: x, target: GetNewKey(x, appSettings)))
                        .ToList()
                        .ForEach(x => copies.TryAdd(x.source, x.target));
                }
                else
                {
                    var responses = await Task.WhenAll(keys.Select(x => RenameAsync(s3Client, x, appSettings)));

                    foreach (var response in responses.Where(x => x.response?.HttpStatusCode == HttpStatusCode.OK || x.response?.HttpStatusCode == HttpStatusCode.Created))
                    {
                        copies.TryAdd(response.source, response.target);
                    }
                }

                Consoler.WriteLines(ConsoleColor.White, $"Batch Renamed: {batchNumber}");

                batchNumber++;

                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            await WriteCsvAsync(copies);

            if (appSettings.Action.Settings.TryGetValue("deleteSource", out string val) &&
                bool.TryParse(val, out bool deleteSource) &&
                deleteSource)
            {
                await DeleteAction.InvokeAsync(copies.Keys, s3Client, appSettings);
            }
        }

        private static async Task<(string source, string target, CopyObjectResponse response)> RenameAsync(IAmazonS3 s3Client, string key, AppSettings appSettings)
        {
            var newKey = GetNewKey(key, appSettings);

            if (key == newKey)
            {
                return (key, newKey, null);
            }

            var response = await s3Client.CopyObjectAsync(appSettings.BucketName, key, appSettings.BucketName, newKey);

            return (key, newKey, response);
        }

        private static string GetNewKey(string key, AppSettings appSettings)
        {
            var find = appSettings.Action.Settings["find"];
            var replace = appSettings.Action.Settings["replace"];

            return Regex.Replace(key, find, replace);
        }

        private static async Task WriteCsvAsync(IDictionary<string, string> copies)
        {
            Consoler.WriteLines(ConsoleColor.Cyan, "Writing renames to CSV...");

            var filePath = Path.Combine(AppSettings.BasePath, $"{AppSettings.OpId}_rename.csv");

            using var fileStream = new FileStream(filePath, FileMode.CreateNew);
            using var streamWriter = new StreamWriter(fileStream, Encoding.UTF8, 1024, leaveOpen: true);
            using var csvWriter = new CsvWriter(streamWriter, CultureInfo.InvariantCulture);

            csvWriter.WriteField("old");
            csvWriter.WriteField("new");

            await csvWriter.NextRecordAsync();

            foreach (var record in copies)
            {
                csvWriter.WriteField(record.Key);
                csvWriter.WriteField(record.Value);

                await csvWriter.NextRecordAsync();
            }
        }
    }
}
