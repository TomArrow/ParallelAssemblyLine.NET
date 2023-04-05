using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ParallelAssemblyLineNET
{

    public class ParallelAssemblyLineOptions
    {
        public int? threadCount = null;
        public TaskCreationOptions threadCreationOptions = 0;
        public bool useNormalTaskScheduler = false;
        public int? inputThreads = null; // If you want input to be parallel instead of sequential, define maximum amount of input threads here.
    }
}
