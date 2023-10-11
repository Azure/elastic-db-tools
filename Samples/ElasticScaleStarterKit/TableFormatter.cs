﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ElasticScaleStarterKit;

/// <summary>
/// Stores tabular data and formats it for writing to output.
/// </summary>
internal class TableFormatter
{
    /// <summary>
    /// Table column names.
    /// </summary>
    private readonly string[] _columnNames;

    /// <summary>
    /// Table rows.
    /// </summary>
    private readonly List<string[]> _rows;

    public TableFormatter(string[] columnNames)
    {
        _columnNames = columnNames;
        _rows = new List<string[]>();
    }

    public void AddRow(object[] values)
    {
        if (values.Length != _columnNames.Length)
        {
            throw new ArgumentException(string.Format("Incorrect number of fields. Expected {0}, actual {1}", _columnNames.Length, values.Length));
        }

        var valueStrings = values.Select(o => o.ToString()).ToArray();

        _rows.Add(valueStrings);
    }

    public override string ToString()
    {
        var output = new StringBuilder();

        // Determine column widths
        var columnWidths = new int[_columnNames.Length];
        for (var c = 0; c < _columnNames.Length; c++)
        {
            var maxValueLength = 0;

            if (_rows.Any())
            {
                maxValueLength = _rows.Select(r => r[c].Length).Max();
            }

            columnWidths[c] = Math.Max(maxValueLength, _columnNames[c].Length);
        }

        // Build format strings that are used to format the column names and fields
        var formatStrings = new string[_columnNames.Length];
        for (var c = 0; c < _columnNames.Length; c++)
        {
            formatStrings[c] = string.Format(" {{0,-{0}}} ", columnWidths[c]);
        }

        // Write header
        for (var c = 0; c < _columnNames.Length; c++)
        {
            _ = output.AppendFormat(formatStrings[c], _columnNames[c]);
        }

        _ = output.AppendLine();

        // Write separator
        for (var c = 0; c < _columnNames.Length; c++)
        {
            _ = output.AppendFormat(formatStrings[c], new string('-', _columnNames[c].Length));
        }

        _ = output.AppendLine();

        // Write rows
        foreach (var row in _rows)
        {
            for (var c = 0; c < _columnNames.Length; c++)
            {
                _ = output.AppendFormat(formatStrings[c], row[c]);
            }

            _ = output.AppendLine();
        }

        return output.ToString();
    }
}
