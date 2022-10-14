using NUnit.Framework;
using ParallelAssemblyLineNET;
using System;

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

            Assert.AreEqual(resultArray,referenceArray);
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

            Assert.AreEqual(resultArray,referenceArray);
            //Assert.Pass();
        }
    }
}