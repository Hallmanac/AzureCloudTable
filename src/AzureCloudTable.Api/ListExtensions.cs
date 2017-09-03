using System.Linq;

namespace System.Collections.Generic
{
    public static class ListExtensions
    {
        /// <summary>
        ///   Takes the current list and returns a List of Lists. A.K.A. a batch of lists where each list is no larger than the
        ///   given <see cref="batchSize" />.
        /// </summary>
        /// <typeparam name="T">Generic type in the List</typeparam>
        /// <param name="currentList">The current list that this method operates on.</param>
        /// <param name="batchSize">The max number of items to be in each list.</param>
        /// <returns></returns>
        public static List<List<T>> ToBatch<T>(this List<T> currentList, int batchSize)
        {
            var batchList = new List<List<T>>();
            var maxBatchCount = currentList.Count < batchSize ? currentList.Count : batchSize;
            var currentCount = 0;
            while (currentCount < currentList.Count)
            {
                var batch = new List<T>();
                batch.AddRange(currentList.Skip(currentCount).Take(maxBatchCount).ToList());
                batchList.Add(batch);
                currentCount += maxBatchCount;
            }
            return batchList;
        }
    }
}
