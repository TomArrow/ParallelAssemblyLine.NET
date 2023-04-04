using System;
using System.Collections.Generic;
using System.Text;

namespace ParallelAssemblyLineNET
{
    public struct ParallelAssemblyLineStatus
    {
        public Int64 InputBufferSize { get; internal set; }
        public Int64 InputBufferSizeMax { get; internal set; }
        public Int64 OutputBufferSize { get; internal set; }
        public Int64 OutputBufferSizeMax { get; internal set; }
        public Int64 FedItems { get; internal set; }
        public Int64 ProcessingItems { get; internal set; }
        public Int64 ProcessedItems { get; internal set; }
        public Int64 DigestedItems { get; internal set; }
    }
}
