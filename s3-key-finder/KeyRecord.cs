using CsvHelper.Configuration.Attributes;

namespace s3_size_finder
{
    public class KeyRecord
    {
        [Index(0)]
        public string Key { get; set; }

        [Index(1)]
        public long? Size { get; set; }
    }
}
