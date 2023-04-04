﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelAssemblyLineNET
{

    /// <summary>
    /// This class helps with quasi-assembly line style situations, in which your input data can only be provided sequentially and single-threadedly and digested (for example written into a file) also sequentially and single-threadedly, but where the processing steps in between can be multithreaded. This makes especially sense in situations where this processing in between is the bottleneck.
    /// </summary>
    /// <typeparam name="TIn">The datatype going INTO the assembly line</typeparam>
    /// <typeparam name="TOut">The datatype LEAVING the assembly line</typeparam>
    public static class ParallelAssemblyLineV2
    {

        public class FeederResult<T> // Nullable wrapper for any data type.
        {
            public T data = default(T);

            public FeederResult(T dataA)
            {
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
        /// <param name="chewer">The chewer receives the data that the feeder provided and the corresponding incrementing number. Multiple chewers work on multiple items in parallel and the output gets buffered..</param>
        /// <param name="digester">The digester receives, in sequential and single-threaded form, the output of the chewers through the buffer and the corresponding incrementing number. It can for example write this data sequentially into a file stream.</param>
        /// <param name="options">Options to define finer points of the behavior of the behavior of this function</param>
        public static void Run<TIn, TOut>(Func<Int64, FeederResult<TIn>> feeder, Func<TIn, Int64, TOut> chewer, Action<TOut, Int64> digester, ParallelAssemblyLineOptions options = null,Action<ParallelAssemblyLineStatus> statusCallback = null)
        {


            int threadCount = (options != null && options.threadCount.HasValue) ? options.threadCount.Value : Environment.ProcessorCount;
            int bufferSize = threadCount * 2;

            AutoResetEvent inputDataResetEvent = new AutoResetEvent(false);
            AutoResetEvent processingResetEvent = new AutoResetEvent(false);
            AutoResetEvent digestingResetEvent = new AutoResetEvent(false);
            AutoResetEvent statusResetEvent = new AutoResetEvent(false);

            ConcurrentDictionary<Int64, FeederResult<TIn>> inputData = new ConcurrentDictionary<long, FeederResult<TIn>>(); // This is the buffer for the input data.
            ConcurrentDictionary<Int64, TOut> processedData = new ConcurrentDictionary<long, TOut>(); // This is the buffer for the processed data. We need to buffer because it might not get finished in correct order
            
            //Int64 nextToDigestIndex = 0;
            //ool allDataProcessing = false;

            Int64 lastItemIndex = -2; // If nothing is sent this could still end up being set to -1, so -2 is safe default value.

            Int64 threadsRunning = 0;


            ParallelAssemblyLineStatus status = new ParallelAssemblyLineStatus() { InputBufferSizeMax = bufferSize, OutputBufferSizeMax = bufferSize };


            Int64 nextToFeedIndex = 0;
            Int64 nextToDigestIndex = 0;
            Int64 processedItemsCount = 0;

            CancellationTokenSource statusCts = new CancellationTokenSource();
            CancellationToken statusCt = statusCts.Token;
            Task statusTask = null;
            bool hasStatusCallback = false;
            if(statusCallback != null)
            {
                statusTask = Task.Factory.StartNew(() => {
                    while (true)
                    {
                        if (statusCt.IsCancellationRequested)
                        {
                            return;
                        }
                        status.InputBufferSize = inputData.Count;
                        status.OutputBufferSize = processedData.Count;
                        status.FedItems = nextToFeedIndex;
                        status.ProcessingItems = threadsRunning;
                        status.ProcessedItems = processedItemsCount;
                        status.DigestedItems = nextToDigestIndex;
                        statusCallback(status);
                        statusResetEvent.WaitOne(); 
                    }
                }, TaskCreationOptions.LongRunning);
                statusTask.ContinueWith((e) => {
                    throw new Exception("Status reporter crashed. " + e.Exception.ToString());
                }, TaskContinuationOptions.OnlyOnFaulted);
                hasStatusCallback = true;
            }


            // Input thread
            Task feederTask = Task.Factory.StartNew(() => {
                while (true)
                {
                    while (inputData.Count < bufferSize)
                    {
                        FeederResult<TIn> inputHere = feeder(nextToFeedIndex);
                        bool success = false;
                        while (!success)
                        {
                            success = inputData.TryAdd(nextToFeedIndex, inputHere);
                        }
                        processingResetEvent.Set();
                        if(hasStatusCallback) statusResetEvent.Set();
                        if (inputHere == null)
                        {
                            lastItemIndex = nextToFeedIndex - 1;
                            digestingResetEvent.Set();
#if DEBUG
                            Debug.WriteLine("DEBUG: Feeding finished.");
#endif
                            return;
                        }
                        nextToFeedIndex++;
                    }
                    inputDataResetEvent.WaitOne(); // Wait until some of the input data has been removed from the input buffer.
                }
            }, TaskCreationOptions.LongRunning);
            feederTask.ContinueWith((e)=> {
                throw new Exception("Feeder crashed. "+e.Exception.ToString());
            },TaskContinuationOptions.OnlyOnFaulted);

            // Processing thread.
            Task processingTask = Task.Factory.StartNew(() => {
                Int64 nextToProcessIndex = 0;
                while (true)
                {
                    while (inputData.ContainsKey(nextToProcessIndex) && Interlocked.Read(ref threadsRunning) < threadCount && processedData.Count < bufferSize) // Only spawn new threads if buffer isn't full and full count of threads to run isn't exhausted.
                    {

                        FeederResult<TIn> inputDataHere = null;
                        bool successOuter2 = false;
                        while (!successOuter2)
                        {
                            successOuter2 = inputData.TryRemove(nextToProcessIndex, out inputDataHere);
                        }
                        inputDataResetEvent.Set();
                        if (hasStatusCallback) statusResetEvent.Set();

                        if (inputDataHere == null)
                        {
                            digestingResetEvent.Set();
#if DEBUG
                            Debug.WriteLine("DEBUG: Processing thread finished.");
#endif
                            return;
                        }

                        Interlocked.Increment(ref threadsRunning);
                        //threadsRunning++;

                        Int64 localIndex = nextToProcessIndex; // Need to do this because otherwise the task will take the state of the more global variable and every thread will just access whatever.

                        // Chewing:
                        Task thisTask = Task.Factory.StartNew(() => {
                            TOut processedDataHere = chewer(inputDataHere, localIndex);
                            inputDataHere = null;
                            bool success = false;
                            while (!success)
                            {
                                success = processedData.TryAdd(localIndex, processedDataHere);
                            }
                            processedItemsCount++;
                            digestingResetEvent.Set();
                            processedDataHere = default(TOut);
                            //threadsRunning--;
                            Interlocked.Decrement(ref threadsRunning);
                            processingResetEvent.Set();
                            if (hasStatusCallback) statusResetEvent.Set();
                        }, options != null ? options.threadCreationOptions : 0);
                        thisTask.ContinueWith((e) => {
                            throw new Exception("Processing task crashed. " + e.Exception.ToString());
                        }, TaskContinuationOptions.OnlyOnFaulted);

                        nextToProcessIndex++;
                    }

                    processingResetEvent.WaitOne();
                }
            }, TaskCreationOptions.LongRunning);
            processingTask.ContinueWith((e) => {
                throw new Exception("Processing crashed. " + e.Exception.ToString());
            }, TaskContinuationOptions.OnlyOnFaulted);

            // Digester thread
            Task digestTask = Task.Factory.StartNew(() => {
                while (true)
                {
                    if (lastItemIndex != -2 && nextToDigestIndex > lastItemIndex)
                    {
#if DEBUG
                        Debug.WriteLine("DEBUG: Digesting finished, before while.");
#endif
                        return;
                    }
                    while (processedData.ContainsKey(nextToDigestIndex))
                    {
                        TOut outputDataHere = default(TOut);
                        bool success = false;
                        while (!success)
                        {
                            success = processedData.TryRemove(nextToDigestIndex, out outputDataHere);
                        }
                        processingResetEvent.Set();
                        if (hasStatusCallback) statusResetEvent.Set();

                        digester(outputDataHere, nextToDigestIndex);
                        nextToDigestIndex++;
                        if (lastItemIndex != -2 && nextToDigestIndex > lastItemIndex) // Sadly doubled here 
                        {
#if DEBUG
                            Debug.WriteLine("DEBUG: Digesting finished, in while.");
#endif
                            return;
                        }
                    }
                    digestingResetEvent.WaitOne(); // Wait until some of the input data has been removed from the input buffer.
                }
            }, TaskCreationOptions.LongRunning);
            digestTask.ContinueWith((e) => {
                throw new Exception("Digester crashed. " + e.Exception.ToString());
            }, TaskContinuationOptions.OnlyOnFaulted);


            feederTask.Wait();
            processingTask.Wait();
            digestTask.Wait();
            if(statusTask != null)
            {
                statusCts.Cancel();
                statusResetEvent.Set();
                statusTask.Wait();
            }
            
        }

    }
}
