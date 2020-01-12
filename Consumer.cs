using System;
using System.Net;
using System.Threading;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Globalization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MySqlCdc;
using MySqlCdc.Events;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;


namespace mysql_replication
{
    public class Consumer : BackgroundService
    {
        protected readonly BinlogClient client;
        protected readonly ILogger logger;
        private readonly JsonSerializerSettings converter = new JsonSerializerSettings()
        {
            Converters = new List<JsonConverter> { new StringEnumConverter() }
        };
        
        private BinlogClient Create(ConnectionOptions option)
        {
            return new BinlogClient(o =>
            {
                o.Binlog = option.Binlog;
                o.Blocking = option.Blocking;
                o.Database = option.Database;
                o.HeartbeatInterval = option.HeartbeatInterval;
                o.Hostname = option.Hostname;
                o.Password = option.Password;
                o.Port = option.Port;
                o.ServerId = option.ServerId;
                o.Username = option.Username;
                o.UseSsl = option.UseSsl;
            });
        }
        public Consumer(IOptions<ConsumerOption> option, ILoggerFactory loggerFactory)
        {
            var o = option.Value;
            this.client = Create(option.Value);
            this.logger = loggerFactory.CreateLogger<Consumer>();
        }
        public async override Task StartAsync(CancellationToken cancellationToken)
        {
            //throw new NotImplementedException();
            await client.ReplicateAsync(async (binlogEvent) =>
            {
                if (binlogEvent is TableMapEvent tableMap)
                {
                    await HandleTableMapEvent(tableMap);
                }
                else if (binlogEvent is WriteRowsEvent writeRows)
                {
                    await HandleWriteRowsEvent(writeRows);
                }
                else if (binlogEvent is UpdateRowsEvent updateRows)
                {
                    await HandleUpdateRowsEvent(updateRows);
                }
                else if (binlogEvent is DeleteRowsEvent deleteRows)
                {
                    await HandleDeleteRowsEvent(deleteRows);
                }
                else await PrintEventAsync(binlogEvent);
            });
        }
        public async override Task StopAsync(CancellationToken cancellationToken)
        {
            //throw new NotImplementedException();
            await Task.CompletedTask;
        }
        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            //throw new NotImplementedException();
            await Task.CompletedTask;
        }

        private async Task PrintEventAsync(IBinlogEvent binlogEvent)
        {
            var json = JsonConvert.SerializeObject(binlogEvent, Formatting.Indented, converter);
            await Task.Run(() => logger.LogInformation(json));
        }

        private async Task HandleTableMapEvent(TableMapEvent tableMap)
        {
            logger.LogInformation($"Processing {tableMap.DatabaseName}.{tableMap.TableName}");
            await PrintEventAsync(tableMap);
        }

        private async Task HandleWriteRowsEvent(WriteRowsEvent writeRows)
        {
            logger.LogInformation($"{writeRows.Rows.Count} rows were written");
            await PrintEventAsync(writeRows);

            foreach (var row in writeRows.Rows)
            {
                // Do something
            }
        }

        private async Task HandleUpdateRowsEvent(UpdateRowsEvent updatedRows)
        {
            logger.LogInformation($"{updatedRows.Rows.Count} rows were updated");
            await PrintEventAsync(updatedRows);

            foreach (var row in updatedRows.Rows)
            {
                var rowBeforeUpdate = row.BeforeUpdate;
                var rowAfterUpdate = row.AfterUpdate;
                // Do something
            }
        }

        private async Task HandleDeleteRowsEvent(DeleteRowsEvent deleteRows)
        {
            logger.LogInformation($"{deleteRows.Rows.Count} rows were deleted");
            await PrintEventAsync(deleteRows);

            foreach (var row in deleteRows.Rows)
            {
                // Do something
            }
        }
    }
}
