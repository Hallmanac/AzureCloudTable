using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace Hallmanac.AzureCloudTable.API
{
    /// <summary>
    /// Helpers to clean invalid characters from a partition key or row key. Contains custom encoders to only encode
    /// characters that are invalid to Azure Table Storage keys.
    /// </summary>
    public class TableKeyEncoder
    {
        private const string EncodedPrefix = "$ENC_";

        /// <summary>
        /// Constructor. Automatically populates the InvalidCharactersMap
        /// </summary>
        public TableKeyEncoder()
        {
            InvalidCharactersMap = GenerateInvalidChactersMap();
        }

        /// <summary>
        /// Map of the invalid characters (key) to the encoded values (string)
        /// </summary>
        public Dictionary<char, string> InvalidCharactersMap { get; set; }


        /// <summary>
        /// Provides a custom encoding of the string if there are any invalid characters.
        /// <para>
        /// If there are no invalid characters the same string value is returned. If there are invalid characters, those 
        /// individual characters are replaced with a custom string value that can be used in a Table Key. The string
        /// will also have a prefix of "$ENC_"
        /// </para>
        /// </summary>
        /// <param name="stringValue"></param>
        /// <returns></returns>
        public string EncodeTableKey(string stringValue)
        {
            // If the string starts with the encoded prefix value then we skip encoding to save time
            if (stringValue.StartsWith(EncodedPrefix))
            {
                return stringValue;
            }

            var invalidChars = InvalidCharactersMap.Keys.ToArray();
            var hasInvalidChars = false;
            var sb = new StringBuilder();
            foreach (var c in stringValue)
            {
                if (!invalidChars.Contains(c))
                {
                    sb.Append(c);
                    continue;
                }
                var mappedChar = InvalidCharactersMap[c];
                sb.Append(mappedChar);
                hasInvalidChars = true;
            }
            if (hasInvalidChars)
            {
                sb.Insert(0, EncodedPrefix);
            }
            return sb.ToString();
        }


        /// <summary>
        /// Decodes a string from a custom encoding for an Azure Table key.
        /// <para>
        /// If the string does not start with the prefix "$ENC_" then the decoding is skipped.
        /// </para>
        /// </summary>
        /// <param name="stringValue"></param>
        /// <returns></returns>
        public string DecodeTableKey(string stringValue)
        {
            // If the string doesn't start with the encoded prefix value then we skip decoding to save time
            if (!stringValue.StartsWith(EncodedPrefix))
            {
                return stringValue;
            }

            // String Builder to hold the final value
            var sb = new StringBuilder();
            for (var i = EncodedPrefix.Length; i < stringValue.Length; i++)
            {
                var charVal = stringValue[i];

                // If we don't have an underscore then we don't have an encoded value
                if (charVal != '_')
                {
                    sb.Append(charVal);
                    continue;
                }

                // Build up string from the next characters starting with the underscore so we can match it against known encodings
                var mappedSb = new StringBuilder();
                mappedSb.Append(charVal);
                
                // iterator
                var nestedIndex = i + 1;

                // The underscore is the begining and ending of the special custom encodings. Since we already have an underscore we're just looking for the next one
                while (nestedIndex < stringValue.Length && stringValue[nestedIndex] != '_')
                {
                    mappedSb.Append(stringValue[nestedIndex]);
                    nestedIndex++;
                }
                if (nestedIndex < stringValue.Length)
                {
                    mappedSb.Append(stringValue[nestedIndex]);
                }
                var mappedValue = mappedSb.ToString();

                // Get a key value pair out of the invalid characters map by matching the values against the built up string
                var mappedCharStringPair = InvalidCharactersMap.FirstOrDefault(x => x.Value == mappedValue);
                
                // If we don't have a match then we continue on
                if (string.IsNullOrEmpty(mappedCharStringPair.Value))
                {
                    sb.Append(charVal);
                    continue;
                }

                // Set the index "i" to the nestedIndex value so that we skip to the character after the encoded ones on the next iteration
                i = nestedIndex;
                sb.Append(mappedCharStringPair.Key);
            }
            var returnValue = sb.ToString();
            return returnValue;
        }



        /// <summary>
        /// Creates the custom mapping of invalid characters to their encoded custom string values
        /// </summary>
        /// <returns></returns>
        public Dictionary<char, string> GenerateInvalidChactersMap()
        {
            var invalidChars = new Dictionary<char, string>
            {
                {'/', "_FS_" },
                {'\\', "_BS_" },
                {'#', "_HT_" },
                {'?', "_QM_" }
            };

            for (var i = 0; i < 32; i++)
            {
                var charString = char.ConvertFromUtf32(i);
                var charValue = Convert.ToChar(charString);
                if (invalidChars.ContainsKey(charValue))
                {
                    continue;
                }
                invalidChars.Add(charValue, $"_C{i}_");
            }

            for (var i = 127; i < 160; i++)
            {
                var charString = char.ConvertFromUtf32(i);
                var charValue = Convert.ToChar(charString);
                if (invalidChars.ContainsKey(charValue))
                {
                    continue;
                }
                invalidChars.Add(charValue, $"_C{i}_");
            }

            return invalidChars;
        }
    }
}
