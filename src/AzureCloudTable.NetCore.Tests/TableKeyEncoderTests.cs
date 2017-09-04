using Hallmanac.AzureCloudTable.API;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AzureCloudTable.NetCore.Tests
{
    [TestClass]
    public class TableKeyEncoderTests
    {
        [TestMethod]
        public void Encoder_Should_Encode_Forward_Slash()
        {
            var encoder = new TableKeyEncoder();
            const string rawString = "xyz/123";
            var mappedEncoding = encoder.InvalidCharactersMap['/'];
            var manualEncoding = $"$ENC_xyz{mappedEncoding}123";

            var encodedString = encoder.EncodeTableKey(rawString);

            Assert.AreEqual(manualEncoding, encodedString);
        }


        [TestMethod]
        public void Encoder_Should_Encode_Back_Slash()
        {
            var encoder = new TableKeyEncoder();
            const string rawString = "xyz\\123";
            var mappedEncoding = encoder.InvalidCharactersMap['\\'];
            var manualEncoding = $"$ENC_xyz{mappedEncoding}123";

            var encodedString = encoder.EncodeTableKey(rawString);

            Assert.AreEqual(manualEncoding, encodedString);
        }


        [TestMethod]
        public void Encoder_Should_Encode_Hash_Tag()
        {
            var encoder = new TableKeyEncoder();
            const string rawString = "xyz#123";
            var mappedEncoding = encoder.InvalidCharactersMap['#'];
            var manualEncoding = $"$ENC_xyz{mappedEncoding}123";

            var encodedString = encoder.EncodeTableKey(rawString);

            Assert.AreEqual(manualEncoding, encodedString);
        }


        [TestMethod]
        public void Encoder_Should_Encode_Question_Mark()
        {
            var encoder = new TableKeyEncoder();
            const string rawString = "xyz?123";
            var mappedEncoding = encoder.InvalidCharactersMap['?'];
            var manualEncoding = $"$ENC_xyz{mappedEncoding}123";

            var encodedString = encoder.EncodeTableKey(rawString);

            Assert.AreEqual(manualEncoding, encodedString);
        }


        [TestMethod]
        public void Encoder_Should_Decode_Forward_Slash_Encoding()
        {
            var encoder = new TableKeyEncoder();
            var mappedEncoding = encoder.InvalidCharactersMap['/'];
            var encodedString = $"$ENC_xyz{mappedEncoding}123";
            var decodedString = encoder.DecodeTableKey(encodedString);
            const string manualDecoded = "xyz/123";

            Assert.AreEqual(manualDecoded, decodedString);
        }


        [TestMethod]
        public void Encoder_Should_Decode_Back_Slash_Encoding()
        {
            var encoder = new TableKeyEncoder();
            var mappedEncoding = encoder.InvalidCharactersMap['\\'];
            var encodedString = $"$ENC_xyz{mappedEncoding}123";
            var decodedString = encoder.DecodeTableKey(encodedString);
            const string manualDecoded = "xyz\\123";

            Assert.AreEqual(manualDecoded, decodedString);
        }


        [TestMethod]
        public void Encoder_Should_Decode_Hash_Tag_Encoding()
        {
            var encoder = new TableKeyEncoder();
            var mappedEncoding = encoder.InvalidCharactersMap['#'];
            var encodedString = $"$ENC_xyz{mappedEncoding}123";
            var decodedString = encoder.DecodeTableKey(encodedString);
            const string manualDecoded = "xyz#123";

            Assert.AreEqual(manualDecoded, decodedString);
        }


        [TestMethod]
        public void Encoder_Should_Decode_Question_Mark_Encoding()
        {
            var encoder = new TableKeyEncoder();
            var mappedEncoding = encoder.InvalidCharactersMap['?'];
            var encodedString = $"$ENC_xyz{mappedEncoding}123";
            var decodedString = encoder.DecodeTableKey(encodedString);
            const string manualDecoded = "xyz?123";

            Assert.AreEqual(manualDecoded, decodedString);
        }


        [TestMethod]
        public void Encoder_Should_Ignore_Regular_Underscores()
        {
            var encoder = new TableKeyEncoder();
            var mappedEncoding = encoder.InvalidCharactersMap['/'];
            var encodedString = $"$ENC_xyz{mappedEncoding}123_22_h";
            var decodedString = encoder.DecodeTableKey(encodedString);
            const string manualDecoded = "xyz/123_22_h";

            Assert.AreEqual(manualDecoded, decodedString);
        }


        [TestMethod]
        public void Encoder_Should_Ignore_Encoded_String()
        {
            var encoder = new TableKeyEncoder();
            var mappedEncoding = encoder.InvalidCharactersMap['/'];
            var encodedString = $"$ENC_xyz{mappedEncoding}123_22_h";
            var reEncodedString = encoder.EncodeTableKey(encodedString);

            Assert.AreEqual(encodedString, reEncodedString);
        }

        [TestMethod]
        public void Encoder_Should_Encode_Control_Characters()
        {
            var encoder = new TableKeyEncoder();
            const string rawString = "xyz\r123";
            var mappedEncoding = encoder.InvalidCharactersMap['\r'];
            var manualEncoding = $"$ENC_xyz{mappedEncoding}123";

            var encodedString = encoder.EncodeTableKey(rawString);

            Assert.AreEqual(manualEncoding, encodedString);
        }
    }
}
