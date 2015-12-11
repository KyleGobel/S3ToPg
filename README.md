# S3ToPg
Bulk insert S3 Docs into PostgreSQL Server easily

1 File dropin or nuget 

Dependencies
```
Npgsql
AWSSDK.S3
```


```cs
S3ToPgConfig.SetConfig(new BasicAWSCredentials("accessKey", "secretKey"), RegionEndpoint.US-West-2, ",");
using (var connection = new NpgsqlConnection(connectionString))
{
  connection.LoadFromS3("s3://mybucket/myfile.csv", "mytable");
}
```
