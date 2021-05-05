using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace s3_size_finder
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                Consoler.WriteLines(ConsoleColor.Cyan, "Hello, world.");

                var configuration = new ConfigurationBuilder()
                       .AddJsonFile($"{GetBasePath()}/appsettings.json", false, false)
                       .Build();

                var appSettings = configuration.Get<AppSettings>();

                using var finder = new Finder(appSettings);

                var filePath = await finder.FindAsync();

                if (string.IsNullOrEmpty(filePath))
                {
                    Consoler.WriteLines(ConsoleColor.Green, "We're all done here.");
                }
                else
                {
                    Consoler.WriteLines(ConsoleColor.Green, $"We're all done here. Results stored at {filePath}");
                }
            }
            catch (Exception ex)
            {
                Consoler.WriteError($"{nameof(Finder.FindAsync)} Failed Unexpectedly.", ex.Message);
            }
            finally
            {
                Console.ReadKey();
            }
        }

        private static string GetBasePath()
        {
            using var processModule = Process.GetCurrentProcess().MainModule;

            return Path.GetDirectoryName(processModule?.FileName);
        }
    }
}
