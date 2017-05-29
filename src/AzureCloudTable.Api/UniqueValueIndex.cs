namespace AzureCloudTableContext.Api
{
    using System;

    public class UniqueValueIndex<TDomainObject> where TDomainObject : class, new()
    {
        private readonly string _givenIndexName;
        private Func<TDomainObject, object> _getUniqueValueFromCriteria;

        /// <summary>
        /// Function that runs on an object to retrieve the value for this index.
        /// </summary>
        public Func<TDomainObject, object> GetUniqueValueFromFilter { get { return _getUniqueValueFromCriteria ?? (givenObj => true); } } 

        /// <summary>
        /// Name of the index provided by the configuration.
        /// </summary>
        public string GivenIndexName { get { return _givenIndexName; } }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="givenIndexName">Name of the index</param>
        public UniqueValueIndex(string givenIndexName)
        {
            _givenIndexName = givenIndexName;
        }

        /// <summary>
        /// Sets the method by which the value is to be retrieved.
        /// </summary>
        /// <param name="filterExpression">A function that takes in the domain object and returns the value of one of its members</param>
        /// <returns></returns>
        public UniqueValueIndex<TDomainObject> SetUniqueValueCriteria(Func<TDomainObject, object> filterExpression)
        {
            _getUniqueValueFromCriteria = filterExpression;
            return this;
        }

        internal string GetHashedUniqueValue(TDomainObject fromDomainObject)
        {
            var obj = GetUniqueValueFromFilter(fromDomainObject);

        }
    }
}