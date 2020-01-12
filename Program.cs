using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace mysql_replication
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var host = new HostBuilder()
                .ConfigureHostConfiguration(c => c.AddEnvironmentVariables())
                .ConfigureAppConfiguration((hostContext, config) =>
                {
                    config.SetBasePath(Directory.GetCurrentDirectory());
                    config.AddEnvironmentVariables();
                    config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
                    config.AddJsonFile($"appsettings.dev.json", optional: true, reloadOnChange: true);
                    config.AddCommandLine(args);
                })
                .ConfigureLogging((hostContext, logging) =>
                {
                    logging.AddSerilog((new LoggerConfiguration()).ReadFrom.Configuration(hostContext.Configuration).CreateLogger(), dispose: true);
                })
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddOptions();

                    services.Configure<ConsumerOption>(option => hostContext.Configuration.GetSection("Consumer").Bind(option));

                    services.AddHostedService<Consumer>();
                })
                .Build();

            using (host)
            {
                await host.StartAsync();

                await host.WaitForShutdownAsync();

                await host.StopAsync();
            }
        }
    }
}
