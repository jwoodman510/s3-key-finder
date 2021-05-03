namespace s3_size_finder
{
    public class AppSettings
    {
        public string Region { get; set; }

        public string AccessKey { get; set; }

        public string SecretKey { get; set; }

        public string BucketName { get; set; }

        public long MinSizeBytes { get; set; }

        public long MaxSizeBytes { get; set; }

        public string KeyPattern { get; set; }
    }
}
