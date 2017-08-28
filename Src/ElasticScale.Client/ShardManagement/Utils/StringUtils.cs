// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement
{
    /// <summary>
    /// Utility methods for string manipulation.
    /// </summary>
    internal static class StringUtils
    {
        /// <summary>
        /// Lookup array for converting byte[] to string.
        /// </summary>
        private static char[] s_byteToCharLookup = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

        /// <summary>
        /// Creates a formatted string which is culture invariant with given arguments.
        /// </summary>
        /// <param name="input">Input string.</param>
        /// <param name="args">Collection of formatting arguments.</param>
        /// <returns>Formatted string.</returns>
        internal static string FormatInvariant(string input, params object[] args)
        {
            return string.Format(CultureInfo.InvariantCulture, input, args);
        }

        /// <summary>
        /// Converts the given byte array to its string representation.
        /// </summary>
        /// <param name="input">Input byte array.</param>
        /// <returns>String representation of the byte array.</returns>
        internal static string ByteArrayToString(byte[] input)
        {
            Debug.Assert(input != null);

            StringBuilder result = new StringBuilder((input.Length + 1) * 2).Append("0x");

            foreach (byte b in input)
            {
                result.Append(s_byteToCharLookup[b >> 4]).Append(s_byteToCharLookup[b & 0x0f]);
            }

            return result.ToString();
        }

        /// <summary>
        /// Converts the given string to its byte array representation.
        /// </summary>
        /// <param name="input">Input string.</param>
        /// <returns>Byte representation of the string.</returns>
        internal static byte[] StringToByteArray(string input)
        {
            Debug.Assert(input != null);

            byte[] result = new byte[(input.Length - 2) / 2];

            for (int i = 2, j = 0; i < input.Length; i += 2, j++)
            {
                result[j] = (byte)(CharToByte(input[i]) * 16 + CharToByte(input[i + 1]));
            }

            return result;
        }

        /// <summary>
        /// Converts given character to its binary representation.
        /// </summary>
        /// <param name="c">Input character.</param>
        /// <returns>Byte representation of input character.</returns>
        private static byte CharToByte(char c)
        {
            switch (c)
            {
                case '0':
                    return 0;
                case '1':
                    return 1;
                case '2':
                    return 2;
                case '3':
                    return 3;
                case '4':
                    return 4;
                case '5':
                    return 5;
                case '6':
                    return 6;
                case '7':
                    return 7;
                case '8':
                    return 8;
                case '9':
                    return 9;
                case 'a':
                case 'A':
                    return 0xa;
                case 'b':
                case 'B':
                    return 0xb;
                case 'c':
                case 'C':
                    return 0xc;
                case 'd':
                case 'D':
                    return 0xd;
                case 'e':
                case 'E':
                    return 0xe;
                case 'f':
                case 'F':
                    return 0xf;
                default:
                    throw new InvalidOperationException("Unexpected byte value.");
            }
        }
    }
}
