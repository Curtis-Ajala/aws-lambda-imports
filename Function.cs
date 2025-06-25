using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Text;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AwsLambdaImports
{
    public class Function
    {
        private readonly IAmazonS3 _s3Client;
        private readonly ILogger<Function> _logger;

        public Function()
        {
            _s3Client = new AmazonS3Client();
            _logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<Function>();
        }

        public async Task<FunctionResponse> FunctionHandler(S3Event s3Event, ILambdaContext context)
        {
            var response = new FunctionResponse
            {
                ProcessedFiles = new List<string>(),
                Errors = new List<string>()
            };

            foreach (var record in s3Event.Records)
            {
                try
                {
                    _logger.LogInformation("Processing S3 event for bucket: {Bucket}, key: {Key}", 
                        record.S3.Bucket.Name, record.S3.Object.Key);

                    // Double check only csv. Bucket is configured to only send csv files.
                    if (!record.S3.Object.Key.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
                    {
                        _logger.LogInformation("Skipping non-CSV file: {Key}", record.S3.Object.Key);
                        continue;
                    }

                    await ProcessCsvFile(record.S3.Bucket.Name, record.S3.Object.Key, response);
                    response.ProcessedFiles.Add(record.S3.Object.Key);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing S3 event for bucket: {Bucket}, key: {Key}", 
                        record.S3.Bucket.Name, record.S3.Object.Key);
                    response.Errors.Add($"Error processing {record.S3.Object.Key}: {ex.Message}");
                }
            }

            return response;
        }

        private async Task ProcessCsvFile(string bucketName, string key, FunctionResponse response)
        {
            _logger.LogInformation("Downloading CSV file from S3: {Bucket}/{Key}", bucketName, key);

            // Download the CSV file from S3
            var getObjectRequest = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = key
            };

            using var getObjectResponse = await _s3Client.GetObjectAsync(getObjectRequest);
            using var reader = new StreamReader(getObjectResponse.ResponseStream);
            
            // Configure CSV reader
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HeaderValidated = null,
                MissingFieldFound = null,
                Delimiter = ",",
                HasHeaderRecord = true
            };

            using var csv = new CsvReader(reader, config);
            
            // Read all rows
            var rows = csv.GetRecords<dynamic>().ToList();
            
            _logger.LogInformation("Successfully read {RecordCount} rows from CSV file: {Key}", 
                rows.Count, key);
            
            foreach (var row in rows)
            {
                // Do something with the data
            }
        }
    }

    public class FunctionResponse
    {
        public List<string> ProcessedFiles { get; set; } = new();
        public List<string> Errors { get; set; } = new();
        public int TotalRecordsProcessed { get; set; }
    }
} 