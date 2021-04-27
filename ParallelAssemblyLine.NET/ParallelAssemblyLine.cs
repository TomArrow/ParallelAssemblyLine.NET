using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ParallelAssemblyLineNET
{

    public class ParallelAssemblyLineOptions
    {

    }

    /// <summary>
    /// This class helps with quasi-assembly line style situations, in which your input data can only be provided sequentially and single-threadedly and digested (for example written into a file) also sequentially and single-threadedly, but where the processing steps in between can be multithreaded. This makes especially sense in situations where this processing in between is the bottleneck.
    /// </summary>
    /// <typeparam name="TIn">The datatype going INTO the assembly line</typeparam>
    /// <typeparam name="TOut">The datatype LEAVING the assembly line</typeparam>
    public static class ParallelAssemblyLine 
    {

        public class FeederResult<T> // Nullable wrapper for any data type.
        {
            public T data = default(T);

            public FeederResult(T dataA){
                data = dataA;
            }

            public static implicit operator T(FeederResult<T> d) => d.data;
            public static implicit operator FeederResult<T>(T data) => new FeederResult<T>(data);
        }

        // 
        /// <summary>
        /// Start a new assembly line. The function finishes once the digester has been called and has finished for the last item.
        /// </summary>
        /// <param name="feeder">The feeder provides the source data which is to be processed. It will only get called single-threadedly and sequentially and provided with an incrementing number starting at zero. You can use this as an index. If you wish to indicate that there are no more items to provide, return null.</param>
        /// <param name="chewer">The chewer receives the data that the feeder provided, but multiple chewers work on multiple items in parallel.</param>
        /// <param name="digester">The digester receives, in sequential and single-threaded form, the output of the chewers. It can for example write this data sequentially into a file stream.</param>
        /// <param name="options">Options to define finer points of the behavior of the behavior of this function</param>
        public static void Assemble<TIn, TOut>(Func<Int64,FeederResult<TIn>> feeder,Func<TIn,Int64,TOut> chewer, Action<TOut,Int64> digester, ParallelAssemblyLineOptions options = null)
        {


            int threadCount = Environment.ProcessorCount;
            int bufferSize = threadCount * 2;

            ConcurrentDictionary<Int64, TOut> processedData = new ConcurrentDictionary<long, TOut>(); // This is the buffer for the processed data. We need to buffer because it might not get finished in correct order
            ConcurrentDictionary<Int64, bool> threadsFinished = new ConcurrentDictionary<long, bool>(); // A dictionary of threads that have finished working, indexed by the iterator.
            ConcurrentDictionary<Int64, bool> threadsRunning = new ConcurrentDictionary<long, bool>(); // A dictionary of threads that are still potentially running, indexed by the iterator.
            ConcurrentDictionary<Int64, Task> runningTasks = new ConcurrentDictionary<long, Task>(); // A dictionary of Tasks that may or may not still be running. Necessary to replace Thread.Sleep() with Task.WaitAny()

            Int64 nextToReadIndex = 0;
            Int64 nextToDigestIndex = 0;
            bool allDataFed = false;
            bool allDataDigested = false;
            while (!allDataDigested)
            {

                if (processedData.Count ==0 && threadsRunning.Count == 0 && allDataFed)
                {
                    allDataDigested = true;
                    break;
                }

                bool noMoreDataForWriting = false;
                while (!noMoreDataForWriting && processedData.Count>0)
                {
                    if(threadsFinished.ContainsKey(nextToDigestIndex) && processedData.ContainsKey(nextToDigestIndex))
                    {
                        TOut resultForDigestion;
                        bool success =  processedData.TryRemove(nextToDigestIndex, out resultForDigestion);
                        if (success)
                        {
                            digester(resultForDigestion, nextToDigestIndex);
                            nextToDigestIndex++;
                        } else
                        {
                            noMoreDataForWriting = true;
                        }

                    }
                    else
                    {
                        noMoreDataForWriting = true;
                    }
                }

                // Only spawn new threads if buffer isn't full and full count of threads to run isn't exhausted.
                while(!allDataFed && threadsRunning.Count < threadCount && processedData.Count < bufferSize)
                {
                    FeederResult<TIn> inputData = feeder(nextToReadIndex);

                    if(inputData == null)
                    {
                        allDataFed = true;
                        break;
                    }

                    bool successOuter = false;
                    while (!successOuter)
                    {
                        successOuter = threadsRunning.TryAdd(nextToReadIndex, true);
                    }
                    

                    Int64 localIndex = nextToReadIndex; // Need to do this because otherwise the task will take the state of the more global variable and every thread will just access whatever.
                    Task thisTask = Task.Run(()=> {
                        TOut processedDataHere = chewer(inputData, localIndex);
                        inputData = null;
                        bool success = false;
                        while (!success)
                        {
                            success = processedData.TryAdd(localIndex, processedDataHere);
                        }
                        processedDataHere = default(TOut); 
                        success = false;
                        while (!success)
                        {
                            success = threadsFinished.TryAdd(localIndex, true);
                        }
                        success = false;
                        while (!success)
                        {
                            success = threadsRunning.TryRemove(localIndex, out _);
                        }
                    });

                    successOuter = false;
                    while (!successOuter)
                    {
                        successOuter = runningTasks.TryAdd(nextToReadIndex, thisTask);
                    }

                    nextToReadIndex++;
                }
                
                List<Task> unfinishedTasks = new List<Task>();
                // Remove already completed tasks from the runningTasks array
                foreach (KeyValuePair<Int64,Task> taskToPossiblyRemove in runningTasks)
                {
                    if (taskToPossiblyRemove.Value.IsCompleted)
                    {
                        bool success = false;
                        while (!success)
                        {
                            success = runningTasks.TryRemove(taskToPossiblyRemove.Key,out _);
                        }

                    }
                    else
                    {
                        unfinishedTasks.Add(taskToPossiblyRemove.Value);
                    }
                }

                if(processedData.Count == 0 && unfinishedTasks.Count > 0)
                {

                    // This is smarter than a sleep because it won't wait a fixed time but just continue whenever at least one Task has finished. No use repeating the loop over and over while there's nothing new to process anyway.
                    Task.WaitAny(unfinishedTasks.ToArray());
                }
                
            }
        }

    }
}
