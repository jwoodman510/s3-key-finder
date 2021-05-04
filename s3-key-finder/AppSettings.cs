using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace s3_size_finder
{
    public class AppSettings
    {
        public static readonly Guid OpId = Guid.NewGuid();

        public static readonly string BasePath = Path.GetDirectoryName(Process.GetCurrentProcess().MainModule.FileName);

        public string Region { get; set; }

        public string AccessKey { get; set; }

        public string SecretKey { get; set; }

        public string BucketName { get; set; }

        public long MinSizeBytes { get; set; }

        public long MaxSizeBytes { get; set; }

        public string KeyPattern { get; set; }

        public Action Action { get; set; }
    }

    public class Action
    {
        public string Name { get; set; }

        public bool DryRun { get; set; }

        public int BatchSize { get; set; }

        public Dictionary<string, string> Settings { get; set; }
    }
}
