using Amazon.S3;
using CsvHelper;
using MoreLinq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace s3_size_finder
{
    public class DeleteAction
    {
        public const string Name = "DELETE";

        public static async Task InvokeAsync(IEnumerable<string> keys, IAmazonS3 s3Client, AppSettings appSettings)
        {
            var deletedKeys = new List<string>();

            var batchNumber = 1;
            var batchSize = appSettings.Action.BatchSize > 0 ? appSettings.Action.BatchSize : 100;

            foreach (var batch in keys.Batch(batchSize))
            {
                Consoler.WriteLines(ConsoleColor.White, $"Deleting Batch: {batchNumber}");

                if (appSettings.Action.DryRun)
                {
                    deletedKeys.AddRange(batch);
                }
                else
                {
                    var response = await s3Client.DeleteObjectsAsync(new Amazon.S3.Model.DeleteObjectsRequest
                    {
                        BucketName = appSettings.BucketName,
                        Objects = keys.Select(key => new Amazon.S3.Model.KeyVersion
                        {
                            Key = key
                        }).ToList()
                    });

                    if (response?.DeleteErrors?.Count > 0)
                    {
                        Consoler.WriteError($"Failed to delete {response.DeleteErrors.Count} objects.");
                    }

                    deletedKeys.AddRange(response.DeletedObjects.Select(x => x.Key));
                }

                Consoler.WriteLines(ConsoleColor.White, $"Batch Deleted: {batchNumber}");

                batchNumber++;

                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            await WriteCsvAsync(keys);
        }

        private static async Task WriteCsvAsync(IEnumerable<string> keys)
        {
            Consoler.WriteLines(ConsoleColor.Cyan, "Writing deletes to CSV...");

            var filePath = Path.Combine(AppSettings.BasePath, $"{AppSettings.OpId}_delete.csv");

            using var fileStream = new FileStream(filePath, FileMode.CreateNew);
            using var streamWriter = new StreamWriter(fileStream, Encoding.UTF8, 1024, leaveOpen: true);
            using var csvWriter = new CsvWriter(streamWriter, CultureInfo.InvariantCulture);

            foreach (var key in keys)
            {
                csvWriter.WriteField(key);

                await csvWriter.NextRecordAsync();
            }
        }
    }
}
