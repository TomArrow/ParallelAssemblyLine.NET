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
        public bool? useNormalTaskScheduler = false;
    }
}
