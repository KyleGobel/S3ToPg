using System;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Text.RegularExpressions;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Jil;
using Npgsql;

namespace S3ToPg
{
    public class S3Manifest
    {
        public S3Entry[] Entries { get; set; }
    }

    public class S3Entry
    {
        public string Url { get; set; }
    }
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
        public static void LoadFromS3(this NpgsqlConnection connection, string s3Path, string tableName, bool gzipped = false, bool manifest = false)
        {
            var s3Loader = new S3Loader();
            var tuple = Utils.ParseS3Path(s3Path);
            if (tuple == null)
            {
                throw new ArgumentException("Couldn't parse s3Path");
            }

            if (manifest)
            {
                var manifestFile = s3Loader.LoadManifest(tuple.Item1, tuple.Item2);
                foreach (var entry in manifestFile.Entries)
                {
                    var tup = Utils.ParseS3Path(entry.Url);
                    if (gzipped)
                    {
                        s3Loader.LoadGzipped(tup.Item1, tup.Item2, S3ToPgConfig.Instance.Delimiter, tableName,
                            connection);
                    }
                    else
                    {
                        s3Loader.Load(tup.Item1, tup.Item2, tableName, S3ToPgConfig.Instance.Delimiter, connection);
                    }
                }
            }
            else
            {
                if (gzipped)
                {
                    s3Loader.LoadGzipped(tuple.Item1, tuple.Item2, S3ToPgConfig.Instance.Delimiter, tableName, connection);
                }
                else
                {
                    s3Loader.Load(tuple.Item1, tuple.Item2, tableName, S3ToPgConfig.Instance.Delimiter, connection);
                }
            }
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

        public S3Manifest LoadManifest(string bucket, string key)
        {
            var s3Obj = _s3Client.GetObject(bucket, key);

            using (var reader = new StreamReader(s3Obj.ResponseStream))
            {
                var manifest = reader.ReadToEnd();
                return JSON.Deserialize<S3Manifest>(manifest, Options.CamelCase);
            }
        }

        public void LoadGzipped(string bucket, string key, string delimiter, string tableName, NpgsqlConnection connection)
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            var s3Obj = _s3Client.GetObject(bucket, key);
            using (var decompressStream = new GZipStream(s3Obj.ResponseStream, CompressionMode.Decompress))
            using (var reader = new StreamReader(decompressStream))
            {
                var copyCommand = string.Format("COPY {0} FROM STDIN (DELIMITER '{1}')", tableName, delimiter);
                using (var writer = connection.BeginTextImport(copyCommand))
                {
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        writer.WriteLine(line);
                    }
                }
            }
        }
    }

    internal class Utils
    {
        public static Tuple<string, string> ParseS3Path(string path)
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