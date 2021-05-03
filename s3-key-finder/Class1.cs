using System;

namespace s3_size_finder
{
    public static class Consoler
    {
        private static readonly object ConsolerLock = new object();

        public static void WriteError(params string[] lines) => WriteLines(ConsoleColor.Red, lines);

        public static void WriteLines(ConsoleColor consoleColor, params string[] lines)
        {
            if (lines.Length == 0)
            {
                return;
            }

            lock (ConsolerLock)
            {
                Console.ForegroundColor = consoleColor;

                foreach (var line in lines)
                {
                    Console.WriteLine(line);
                }

                Console.ForegroundColor = ConsoleColor.White;
            }
        }
    }
}
