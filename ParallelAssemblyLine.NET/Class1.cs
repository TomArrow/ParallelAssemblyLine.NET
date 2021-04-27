using System;

namespace ParallelAssemblyLine.NET
{

    public class ParallelAssemblyLineOptions
    {

    }

    /// <summary>
    /// This class helps with quasi-assembly line style situations, in which your input data can only be provided sequentially and single-threadedly and digested (for example written into a file) also sequentially and single-threadedly, but where the processing steps in between can be multithreaded. This makes especially sense in situations where this processing in between is the bottleneck.
    /// </summary>
    /// <typeparam name="TIn">The datatype going INTO the assembly line</typeparam>
    /// <typeparam name="TOut">The datatype LEAVING the assembly line</typeparam>
    public static class ParallelAssemblyLine<TIn, TOut>
    {

        // 
        /// <summary>
        /// Start a new assembly line. The function finishes once the digester has been called and has finished for the last item.
        /// </summary>
        /// <param name="feeder">The feeder provides the source data which is to be processed. It will only get called single-threadedly and sequentially and provided with an incrementing number. You can use this as an index. If you wish to indicate that there are no more items to provide, return null.</param>
        /// <param name="chewer">The chewer receives the data that the feeder provided, but multiple chewers work on multiple items in parallel.</param>
        /// <param name="digester">The digester receives, in sequential and single-threaded form, the output of the chewers. It can for example write this data sequentially into a file stream.</param>
        /// <param name="options">Options to define finer points of the behavior of the behavior of this function</param>
        public static void Assemble(Func<Int64,TIn> feeder,Func<TIn,TOut> chewer, Action<TOut> digester, ParallelAssemblyLineOptions options = null)
        {
            int threadCount = Environment.ProcessorCount;
            int bufferSize = threadCount * 2;


            while (true)
            {


            }
        }
    }
}
