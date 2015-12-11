# S3ToPg
Bulk insert S3 Docs into PostgreSQL Server easily


```cs
S3ToPgConfig.SetConfig(new BasicAWSCredentials("accessKey", "secretKey"), RegionEndpoint.US-West-2, ",");
using (var connection = new NpgsqlConnection(connectionString))
{
  connection.LoadFromS3("s3://mybucket/myfile.csv", "mytable");
}
```
