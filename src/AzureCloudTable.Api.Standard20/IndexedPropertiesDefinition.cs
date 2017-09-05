using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hallmanac.AzureCloudTable.Api
{
    /// <summary>
    /// Holds a definition for which properties should be indexed for searching
    /// </summary>
    /// <typeparam name="TDomainObject"></typeparam>
    public class IndexedPropertiesDefinition<TDomainObject> where TDomainObject : class, new()
    {
        
    }
}
