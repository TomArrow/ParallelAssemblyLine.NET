using NUnit.Framework;
using ParallelAssemblyLineNET;
using System;
using System.Threading.Tasks;

namespace ParallelAssemblyLineNET.Tests
{
    public class Tests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void SimpleFunctionalityTest()
        {
            double[] testArray = new double[10] {1,1.5,2,2.6,3,34.5,1.2,34.4, System.Math.PI, 8.2};

            string[] resultArray = new string[testArray.Length];
            string[] referenceArray = new string[testArray.Length];

            for(int i = 0; i < referenceArray.Length; i++)
            {
                referenceArray[i] = testArray[i].ToString();
            }

            ParallelAssemblyLine.Run<double, string>(
                (i) => {
                    if (i < testArray.Length)
                    {
                        return testArray[(int)i];
                    } else
                    {
                        return null;
                    }
                }, 
                (a,i) => { return a.ToString(); }, 
                (a,i) => { resultArray[i] = a; }
            );

            Assert.AreEqual(referenceArray,resultArray);
            //Assert.Pass();
        }

        [Test]
        public void V2SimpleFunctionalityTest()
        {
            double[] testArray = new double[10] {1,1.5,2,2.6,3,34.5,1.2,34.4, System.Math.PI, 8.2};

            string[] resultArray = new string[testArray.Length];
            string[] referenceArray = new string[testArray.Length];

            for(int i = 0; i < referenceArray.Length; i++)
            {
                referenceArray[i] = testArray[i].ToString();
            }

            ParallelAssemblyLineV2.Run<double, string>(
                (i) => {
                    if (i < testArray.Length)
                    {
                        return testArray[(int)i];
                    } else
                    {
                        return null;
                    }
                }, 
                (a,i) => { return a.ToString(); }, 
                (a,i) => { resultArray[i] = a; }
            );

            Assert.AreEqual(referenceArray,resultArray);
            //Assert.Pass();
        }

        [Test]
        public void V2SimpleFunctionalityWithStatusTest()
        {
            double[] testArray = new double[10] {1,1.5,2,2.6,3,34.5,1.2,34.4, System.Math.PI, 8.2};

            string[] resultArray = new string[testArray.Length];
            string[] referenceArray = new string[testArray.Length];

            for(int i = 0; i < referenceArray.Length; i++)
            {
                referenceArray[i] = testArray[i].ToString();
            }

            ParallelAssemblyLineV2.Run<double, string>(
                (i) => {
                    if (i < testArray.Length)
                    {
                        return testArray[(int)i];
                    } else
                    {
                        return null;
                    }
                }, 
                (a,i) => { return a.ToString(); }, 
                (a,i) => { resultArray[i] = a; }
            , null, (status) => {
                TestContext.WriteLine($"inbuf: {status.InputBufferSize},outbuf: {status.OutputBufferSize},finished: {status.DigestedItems},active: {status.ProcessingItems}\n");
            });

            Assert.AreEqual(referenceArray,resultArray);
            //Assert.Pass();
        }

        [Test]
        public void SimpleButLongerFunctionalityTest()
        {
            int countOfItemsToTest = 20000;
            
            Random rnd = new Random();

            double[] testArray = new double[countOfItemsToTest];

            for(int i = 0; i < countOfItemsToTest; i++)
            {
                testArray[i] = rnd.NextDouble();
            }


            string[] resultArray = new string[testArray.Length];
            string[] referenceArray = new string[testArray.Length];

            for(int i = 0; i < referenceArray.Length; i++)
            {
                referenceArray[i] = testArray[i].ToString();
            }

            ParallelAssemblyLine.Run<double, string>(
                (i) => {
                    if (i < testArray.Length)
                    {
                        return testArray[(int)i];
                    } else
                    {
                        return null;
                    }
                }, 
                (a,i) => { return a.ToString(); }, 
                (a,i) => { resultArray[i] = a; }
            );

            Assert.AreEqual(referenceArray, resultArray);
            //Assert.Pass();
        }
        [Test]
        public void V2SimpleButLongerFunctionalityTest()
        {
            int countOfItemsToTest = 20000;
            
            Random rnd = new Random();

            double[] testArray = new double[countOfItemsToTest];

            for(int i = 0; i < countOfItemsToTest; i++)
            {
                testArray[i] = rnd.NextDouble();
            }


            string[] resultArray = new string[testArray.Length];
            string[] referenceArray = new string[testArray.Length];

            for(int i = 0; i < referenceArray.Length; i++)
            {
                referenceArray[i] = testArray[i].ToString();
            }

            ParallelAssemblyLineV2.Run<double, string>(
                (i) => {
                    if (i < testArray.Length)
                    {
                        return testArray[(int)i];
                    } else
                    {
                        return null;
                    }
                }, 
                (a,i) => { return a.ToString(); }, 
                (a,i) => { resultArray[i] = a; }
            );

            Assert.AreEqual(referenceArray, resultArray);
            //Assert.Pass();
        }

        [Test]
        public void V2SimpleButLongerFunctionalityWithStatusTest()
        {
            int countOfItemsToTest = 20000;
            
            Random rnd = new Random();

            double[] testArray = new double[countOfItemsToTest];

            for(int i = 0; i < countOfItemsToTest; i++)
            {
                testArray[i] = rnd.NextDouble();
            }


            string[] resultArray = new string[testArray.Length];
            string[] referenceArray = new string[testArray.Length];

            for(int i = 0; i < referenceArray.Length; i++)
            {
                referenceArray[i] = testArray[i].ToString();
            }

            ParallelAssemblyLineV2.Run<double, string>(
                (i) => {
                    if (i < testArray.Length)
                    {
                        return testArray[(int)i];
                    } else
                    {
                        return null;
                    }
                }, 
                (a,i) => { return a.ToString(); }, 
                (a,i) => { resultArray[i] = a; }
            ,null,(status)=> {
                TestContext.WriteLine($"inbuf: {status.InputBufferSize},outbuf: {status.OutputBufferSize},finished: {status.DigestedItems},active: {status.ProcessingItems}\n");
            });

            Assert.AreEqual(referenceArray, resultArray);
            //Assert.Pass();
        }

        [Test]
        public void V2SimpleButLongerFunctionalityLongRunningTest()
        {
            int countOfItemsToTest = 20000;
            
            Random rnd = new Random();

            double[] testArray = new double[countOfItemsToTest];

            for(int i = 0; i < countOfItemsToTest; i++)
            {
                testArray[i] = rnd.NextDouble();
            }


            string[] resultArray = new string[testArray.Length];
            string[] referenceArray = new string[testArray.Length];

            for(int i = 0; i < referenceArray.Length; i++)
            {
                referenceArray[i] = testArray[i].ToString();
            }

            ParallelAssemblyLineV2.Run<double, string>(
                (i) => {
                    if (i < testArray.Length)
                    {
                        return testArray[(int)i];
                    } else
                    {
                        return null;
                    }
                }, 
                (a,i) => { return a.ToString(); }, 
                (a,i) => { resultArray[i] = a; }
            ,new ParallelAssemblyLineOptions() { threadCreationOptions = TaskCreationOptions.LongRunning });

            Assert.AreEqual(referenceArray, resultArray);
            //Assert.Pass();
        }
        [Test]
        public void V2SimpleButLongerFunctionalityLongRunningWithRandomDelayTest()
        {
            int countOfItemsToTest = 2000;
            
            Random rnd = new Random();

            double[] testArray = new double[countOfItemsToTest];
            int[,] timesToWait = new int[countOfItemsToTest, 3];

            for (int i = 0; i < countOfItemsToTest; i++)
            {
                testArray[i] = rnd.NextDouble();
                timesToWait[i, 0] =  rnd.Next(0,1);
                timesToWait[i, 1] = rnd.Next(0,50);
                timesToWait[i, 2] = rnd.Next(0,1);
            }


            string[] resultArray = new string[testArray.Length];
            string[] referenceArray = new string[testArray.Length];

            for(int i = 0; i < referenceArray.Length; i++)
            {
                referenceArray[i] = testArray[i].ToString();
            }

            ParallelAssemblyLineV2.Run<double, string>(
                (i) => {
                    if (i < testArray.Length)
                    {
                        System.Threading.Thread.Sleep(timesToWait[i, 0]);
                        return testArray[(int)i];
                    } else
                    {
                        return null;
                    }

                }, 
                (a,i) => {
                    System.Threading.Thread.Sleep(timesToWait[i, 1]);
                    return a.ToString();
                }, 
                (a,i) => {
                    System.Threading.Thread.Sleep(timesToWait[i, 2]);
                    resultArray[i] = a; 
                }
            ,new ParallelAssemblyLineOptions() { threadCreationOptions = TaskCreationOptions.LongRunning }, (status) => {
                TestContext.WriteLine($"inbuf: {status.InputBufferSize},outbuf: {status.OutputBufferSize},finished: {status.DigestedItems},active: {status.ProcessingItems}\n");
            });

            Assert.AreEqual(referenceArray, resultArray);
            //Assert.Pass();
        }
        [Test]
        public void V2SimpleButLongerFunctionalityLongRunningWithRandomDelayParallelInputTest()
        {
            int countOfItemsToTest = 2000;
            
            Random rnd = new Random();

            double[] testArray = new double[countOfItemsToTest];
            int[,] timesToWait = new int[countOfItemsToTest, 3];

            for (int i = 0; i < countOfItemsToTest; i++)
            {
                testArray[i] = rnd.NextDouble();
                timesToWait[i, 0] =  rnd.Next(0,3);
                timesToWait[i, 1] = rnd.Next(0,50);
                timesToWait[i, 2] = rnd.Next(0,1);
            }


            string[] resultArray = new string[testArray.Length];
            string[] referenceArray = new string[testArray.Length];

            for(int i = 0; i < referenceArray.Length; i++)
            {
                referenceArray[i] = testArray[i].ToString();
            }

            ParallelAssemblyLineV2.Run<double, string>(
                (i) => {
                    if (i < testArray.Length)
                    {
                        System.Threading.Thread.Sleep(timesToWait[i, 0]);
                        return testArray[(int)i];
                    } else
                    {
                        return null;
                    }

                }, 
                (a,i) => {
                    System.Threading.Thread.Sleep(timesToWait[i, 1]);
                    return a.ToString();
                }, 
                (a,i) => {
                    System.Threading.Thread.Sleep(timesToWait[i, 2]);
                    resultArray[i] = a; 
                }
            ,new ParallelAssemblyLineOptions() { threadCreationOptions = TaskCreationOptions.LongRunning,inputThreads=8 }, (status) => {
                TestContext.WriteLine($"inbuf: {status.InputBufferSize},outbuf: {status.OutputBufferSize},finished: {status.DigestedItems},active: {status.ProcessingItems}\n");
            });

            Assert.AreEqual(referenceArray, resultArray);
            //Assert.Pass();
        }
        [Test]
        public void V2SimpleButLongerFunctionalityLongRunningWithRandomDelayMoreThreadsTest()
        {
            int countOfItemsToTest = 2000;
            
            Random rnd = new Random();

            double[] testArray = new double[countOfItemsToTest];
            int[,] timesToWait = new int[countOfItemsToTest, 3];

            for (int i = 0; i < countOfItemsToTest; i++)
            {
                testArray[i] = rnd.NextDouble();
                timesToWait[i, 0] =  rnd.Next(0,1);
                timesToWait[i, 1] = rnd.Next(0,50);
                timesToWait[i, 2] = rnd.Next(0,1);
            }


            string[] resultArray = new string[testArray.Length];
            string[] referenceArray = new string[testArray.Length];

            for(int i = 0; i < referenceArray.Length; i++)
            {
                referenceArray[i] = testArray[i].ToString();
            }

            ParallelAssemblyLineV2.Run<double, string>(
                (i) => {
                    if (i < testArray.Length)
                    {
                        System.Threading.Thread.Sleep(timesToWait[i, 0]);
                        return testArray[(int)i];
                    } else
                    {
                        return null;
                    }

                }, 
                (a,i) => {
                    System.Threading.Thread.Sleep(timesToWait[i, 1]);
                    return a.ToString();
                }, 
                (a,i) => {
                    System.Threading.Thread.Sleep(timesToWait[i, 2]);
                    resultArray[i] = a; 
                }
            ,new ParallelAssemblyLineOptions() {threadCount=64, threadCreationOptions = TaskCreationOptions.LongRunning }, (status) => {
                TestContext.WriteLine($"inbuf: {status.InputBufferSize},outbuf: {status.OutputBufferSize},finished: {status.DigestedItems},active: {status.ProcessingItems}\n");
            });

            Assert.AreEqual(referenceArray, resultArray);
            //Assert.Pass();
        }
        
        [Test]
        public void V2SimpleButLongerFunctionalityLongRunningWithRandomDelayDefaultSchedulerTest()
        {
            int countOfItemsToTest = 2000;
            
            Random rnd = new Random();

            double[] testArray = new double[countOfItemsToTest];
            int[,] timesToWait = new int[countOfItemsToTest, 3];

            for (int i = 0; i < countOfItemsToTest; i++)
            {
                testArray[i] = rnd.NextDouble();
                timesToWait[i, 0] =  rnd.Next(0,1);
                timesToWait[i, 1] = rnd.Next(0,50);
                timesToWait[i, 2] = rnd.Next(0,1);
            }


            string[] resultArray = new string[testArray.Length];
            string[] referenceArray = new string[testArray.Length];

            for(int i = 0; i < referenceArray.Length; i++)
            {
                referenceArray[i] = testArray[i].ToString();
            }

            ParallelAssemblyLineV2.Run<double, string>(
                (i) => {
                    if (i < testArray.Length)
                    {
                        System.Threading.Thread.Sleep(timesToWait[i, 0]);
                        return testArray[(int)i];
                    } else
                    {
                        return null;
                    }

                }, 
                (a,i) => {
                    System.Threading.Thread.Sleep(timesToWait[i, 1]);
                    return a.ToString();
                }, 
                (a,i) => {
                    System.Threading.Thread.Sleep(timesToWait[i, 2]);
                    resultArray[i] = a; 
                }
            ,new ParallelAssemblyLineOptions() { useNormalTaskScheduler=true ,threadCreationOptions = TaskCreationOptions.LongRunning }, (status) => {
                TestContext.WriteLine($"inbuf: {status.InputBufferSize},outbuf: {status.OutputBufferSize},finished: {status.DigestedItems},active: {status.ProcessingItems}\n");
            });

            Assert.AreEqual(referenceArray, resultArray);
            //Assert.Pass();
        }
    }
}