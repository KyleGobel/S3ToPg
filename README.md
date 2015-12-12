# S3ToPg
Bulk insert S3 Docs into PostgreSQL Server easily

1 File dropin or nuget 

Dependencies
```
Npgsql
AWSSDK.S3
Jil
```

### Set Credentials, S3 Region and Delimiter

```cs
S3ToPgConfig.SetConfig(new BasicAWSCredentials("accessKey", "secretKey"), RegionEndpoint.US-West-2, ",");
```


### Bulk load a CSV file into the table ``mytable`` 
```cs
using (var connection = new NpgsqlConnection(connectionString))
{
  connection.LoadFromS3("s3://mybucket/myfile.csv", "mytable");
}
```


### Bulk load a gzipped pipe delimited file

```cs
S3ToPgConfig.SetConfig(new BasicAWSCredentials("accessKey", "secretKey"), RegionEndpoint.US-West-2, "|");
using (var connection = new NpgsqlConnection(connectionString))
{
  connection.LoadFromS3("s3://mybucket/myfile.gz", "mytable", true);
}
```


### Bulk load gzipped files with a manifest

```cs
S3ToPgConfig.SetConfig(new BasicAWSCredentials("accessKey", "secretKey"), RegionEndpoint.US-West-2, "|");
using (var connection = new NpgsqlConnection(connectionString))
{
  connection.LoadFromS3("s3://mybucket/export/myfiles_manifest", "mytable", true, true);
}
```

Example Manifest
```json
{
  "entries": [
    {"url":"s3://mybucket/export/yyyy=2015/mm=12/dd=11/hh=22/dimen_0000_part_00.gz"},
    {"url":"s3://mybucket/export/yyyy=2015/mm=12/dd=11/hh=22/dimen_0001_part_00.gz"},
    {"url":"s3://mybucket/export/yyyy=2015/mm=12/dd=11/hh=22/dimen_0002_part_00.gz"},
    {"url":"s3://mybucket/export/yyyy=2015/mm=12/dd=11/hh=22/dimen_0003_part_00.gz"}
  ]
}

```
