using System;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Npgsql;

namespace S3ToPg
{
    public class S3ToPgConfig
    {
        public AWSCredentials Credentials { get; set; }
        public RegionEndpoint Region { get; set; }
        public string Delimiter { get; set; }
        public static void SetConfig(AWSCredentials credentials, RegionEndpoint region, string delimiter)
        {
            _instance = new S3ToPgConfig
            {
                Credentials = credentials,
                Region = region,
                Delimiter = delimiter
            };
        }
        private S3ToPgConfig() { }

        private static S3ToPgConfig _instance;
        public static S3ToPgConfig Instance => _instance ?? (_instance = new S3ToPgConfig());
    }


    public static class S3LoaderExtensions
    {
        public static void LoadFromS3(this NpgsqlConnection connection, string s3Path, string tableName)
        {
            var s3Loader = new S3Loader();
            s3Loader.Load(s3Path, tableName, connection);
        }
    }
    public class S3Loader
    {
        private readonly S3ToPgConfig _config;
        private readonly AmazonS3Client _s3Client;
        public S3Loader(S3ToPgConfig config)

        {
            _config = config;
            _s3Client = new AmazonS3Client(config.Credentials, config.Region);
        }

        public S3Loader()
        {
            _config = S3ToPgConfig.Instance;
            _s3Client = new AmazonS3Client(_config.Credentials, _config.Region);
        }

        public void Load(string bucket, string key, string tableName, NpgsqlConnection connection)
        {
            Load(bucket, key, tableName, _config.Delimiter, connection);
        }
        public void Load(string bucket, string key, string tableName, string delimiter, NpgsqlConnection connection)
        {
            var needsOpening = connection.State != ConnectionState.Open;

            if (needsOpening)
            {
                connection.Open();
            }
            var s3Obj = _s3Client.GetObject(bucket, key);
            using (var s3Reader = new StreamReader(s3Obj.ResponseStream))
            {
                var copyCommand = string.Format("COPY {0} FROM STDIN (DELIMITER '{1}')", tableName, delimiter);
                using (var writer = connection.BeginTextImport(copyCommand))
                {
                    while (!s3Reader.EndOfStream)
                    {
                        var line = s3Reader.ReadLine();
                        writer.WriteLine(line);
                    }
                }
            }
        }

        public void Load(string s3Path, string tableName, NpgsqlConnection connection)
        {
            var tuple = ParseS3Path(s3Path);
            if (tuple == null)
            {
                throw new ArgumentException($"Couldn't parse s3 path '${s3Path}'");
            }
            Load(tuple.Item1, tuple.Item2, tableName, _config.Delimiter, connection);
        }

        private Tuple<string, string> ParseS3Path(string path)
        {
            const string pattern = @"s3://(?<BucketName>[^/]*)/(?<FolderName>.*)$";
            var match = Regex.Match(path, pattern);

            if (match.Success)
            {
                return Tuple.Create(match.Groups["BucketName"].Value, match.Groups["FolderName"].Value);
            }
            return null;
        }
    }
}