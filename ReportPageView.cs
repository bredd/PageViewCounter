using System;
using System.IO;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Data.Tables;
using Azure;

namespace PageViewCounter
{
    public static class ReportPageView
    {
        volatile static int s_queueCount;
        static System.Collections.Concurrent.ConcurrentQueue<string> s_queue = new ConcurrentQueue<string>();
        static ILogger s_log;

        [FunctionName("rpv")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            if (s_log == null)
            {
                s_log = log;
            }

            string url = null;
            if (req.Method == "GET")
            {
                url = req.Query["url"];
            }
            else if (req.Method == "POST")
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                if (req.ContentType.StartsWith("text/plain"))
                {
                    url = requestBody;
                }
                else if (req.ContentType.StartsWith("application/json"))
                {
                    dynamic data = JsonConvert.DeserializeObject(requestBody);
                    url = data ?? data?.url;
                }
            }

            log.LogInformation($"{req.Method} rpv {(url ?? "(null)")}");

            string key;
            if (Uri.TryCreate(url, UriKind.Absolute, out Uri uri))
            {
                key = uri.Host.ToLowerInvariant();
            }
            else
            {
                key = url.ToLowerInvariant();
            }

            // Too bad the Enqueue function doesn't indicate whether this is the
            // first entry into the queue. Instead, we have to do it this way.
            int queueCount = Interlocked.Increment(ref s_queueCount);
            s_queue.Enqueue(key);

            if (queueCount == 1)
            {
                /* discard */_ = Task.Factory.StartNew(RecordViewsFromQueue);
            }

            return new OkObjectResult("\u2713"); // Check mark
        }

        const string c_tableName = "PageViewCounts";

        static void RecordViewsFromQueue()
        {
#if DEBUG
            s_log.LogInformation("Worker thread start.");
#endif
            var connectionString = Environment.GetEnvironmentVariable("storage_connectionString");

            var client = new TableClient(connectionString, c_tableName);

            var month = DateTime.UtcNow.ToString("yyyy-MM");

            for (; ; )
            {
                var queueCount = Interlocked.Decrement(ref s_queueCount);
                if (s_queue.TryDequeue(out string key))
                {
                    var count = IncrementCount(client, key, month);
#if DEBUG
                    s_log.LogInformation($"{key} {month} {count}");
#endif
                }
                else
                {
                    // This should not happen if I've coded this right.
                    s_log.LogError("Race condition: Queue is empty when it should have at least one item.");
                }

                if (queueCount < 0)
                {
                    // This should not happen if I've coded this right.
                    s_log.LogError("Race condition: Queue count less than zero.");
                }

                // Make sure exiting the worker thread corresponds to the transition from 1 to 0.
                // just as entering the thread corresponds to the transition from 0 to 1.
                if (queueCount == 0) break;
            }
#if DEBUG
            s_log.LogInformation("Worker thread end.");
#endif
        }

        static int IncrementCount(TableClient client, string partitionKey, string rowKey)
        {
            PageViewCount counter = null;

            var results = client.Query<PageViewCount>(e => e.PartitionKey == partitionKey && e.RowKey == rowKey);
            using (var i = results.GetEnumerator())
            {
                if (i.MoveNext())
                {
                    counter = i.Current;
                }
            }

            if (counter != null)
            {
                counter.Count++;
                client.UpdateEntity(counter, counter.ETag);
            }
            else
            {
                counter = new PageViewCount()
                {
                    PartitionKey = partitionKey,
                    RowKey = rowKey,
                    Count = 1
                };
                client.AddEntity(counter);
            }

            return counter.Count;
        }

        class PageViewCount : ITableEntity
        {
            public string PartitionKey { get; set; }
            public string RowKey { get; set; }
            public DateTimeOffset? Timestamp { get; set; }
            public ETag ETag { get; set; }
            public Int32 Count { get; set; }
        }
    }
}
