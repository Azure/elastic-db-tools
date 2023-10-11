// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace ElasticScaleStarterKit;

internal static class ConsoleUtils
{
    /// <summary>
    /// Writes detailed information to the console.
    /// </summary>
    public static void WriteInfo(string format, params object[] args) => WriteColor(ConsoleColor.DarkGray, "\t" + format, args);

    /// <summary>
    /// Writes warning text to the console.
    /// </summary>
    public static void WriteWarning(string format, params object[] args) => WriteColor(ConsoleColor.Yellow, format, args);

    /// <summary>
    /// Writes colored text to the console.
    /// </summary>
    public static void WriteColor(ConsoleColor color, string format, params object[] args)
    {
        var oldColor = Console.ForegroundColor;
        Console.ForegroundColor = color;
        Console.WriteLine(format, args);
        Console.ForegroundColor = oldColor;
    }

    /// <summary>
    /// Reads an integer from the console.
    /// </summary>
    public static int ReadIntegerInput(string prompt) => ReadIntegerInput(prompt, allowNull: false).Value;

    /// <summary>
    /// Reads an integer from the console, or returns null if the user enters nothing and allowNull is true.
    /// </summary>
    public static int? ReadIntegerInput(string prompt, bool allowNull)
    {
        while (true)
        {
            Console.Write(prompt);
            var line = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(line) && allowNull)
            {
                return null;
            }

            if (int.TryParse(line, out var inputValue))
            {
                return inputValue;
            }
        }
    }

    /// <summary>
    /// Reads an integer from the console.
    /// </summary>
    public static int ReadIntegerInput(string prompt, int defaultValue, Func<int, bool> validator)
    {
        while (true)
        {
            var input = ReadIntegerInput(prompt, allowNull: true);

            if (!input.HasValue)
            {
                // No input, so return default
                return defaultValue;
            }
            else
            {
                // Input was provided, so validate it
                if (validator(input.Value))
                {
                    // Validation passed, so return
                    return input.Value;
                }
            }
        }
    }
}
