using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reactive;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace ZookeeperLocker.Test
{
    public class LockerTest
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public LockerTest(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void TestSingleLockName()
        {
            int a = 0;
            Parallel.For(0, 30, _ =>
            {
                var zkLocker = new ZkLocker(new ZkOption()
                    {ConnectionsString = "192.168.1.246:2181", SessionTimeout = 30000});
                zkLocker.Lock();
                var b = a;
                a++;
                Assert.True(a - b == 1);
                zkLocker.UnLock();
            });
        }

        [Fact]
        public void TestMultiLockName()
        {
            var currentBag = new ConcurrentBag<int>();
            Parallel.For(0, 10, _ =>
            {
                Task.WhenAll(Task.Run(() =>
                {
                    var zkLocker = new ZkLocker("lockOne", new ZkOption()
                        {ConnectionsString = "192.168.1.246:2181", SessionTimeout = 30000});
                    try
                    {
                        zkLocker.Lock();
                        currentBag.Add(1);
                        Thread.Sleep(10);
                        currentBag.Add(11);
                        zkLocker.UnLock();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }), Task.Run(() =>
                {
                    var zkLocker = new ZkLocker("lockTwo", new ZkOption()
                        {ConnectionsString = "192.168.1.246:2181", SessionTimeout = 30000});
                    try
                    {
                        zkLocker.Lock();
                        currentBag.Add(2);
                        Thread.Sleep(10);
                        currentBag.Add(22);
                        zkLocker.UnLock();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                })).Wait();
            });
            var list = currentBag.ToList();
            
            for (int i = 0; i < list.Count; i++)
            {
                if (Math.Abs(list[i] - list[i + 1]) % 2 == 1)
                {
                    break;
                }
                Assert.True(i < list.Count - 1);
            }
        }

        [Fact]
        public void TestLockTimeout()
        {
            Stopwatch sp = Stopwatch.StartNew();
            sp.Restart();
            Parallel.For(0, 30, _ =>
            {
                var zkLocker = new ZkLocker(new ZkOption()
                    { ConnectionsString = "192.168.1.246:2181", SessionTimeout = 30000 }, int.MaxValue);
                try
                {
                    zkLocker.Lock();
                    _testOutputHelper.WriteLine("1");
                    zkLocker.UnLock();
                }
                catch (LockerTimeoutException ex)
                {
                    _testOutputHelper.WriteLine(ex.Message);
                }
            });
            sp.Stop();
            _testOutputHelper.WriteLine($"{sp.ElapsedMilliseconds}");
        }

        [Fact]
        public void TestLazySingle()
        {
            Lazy<int> a;
            Parallel.For(0, 30, _ =>
            {
                a = new Lazy<int>(() => _, true);
                _testOutputHelper.WriteLine(a.Value.ToString());
            });
        }

        [Fact]
        public void TestHighPerformance()
        {
            Stopwatch sp = Stopwatch.StartNew();
            sp.Restart();
            ZkLockerManager.Configure(new ZkOption()
                { ConnectionsString = "192.168.1.246:2181", SessionTimeout = 30000 });
            Parallel.For(0, 30, _ =>
            {
                var zkLocker = ZkLockerManager.GetLocker("asd", timeout:int.MaxValue);
                try
                {
                    zkLocker.Lock();
                    _testOutputHelper.WriteLine("1");
                    zkLocker.UnLock();
                }
                catch (LockerTimeoutException ex)
                {
                    _testOutputHelper.WriteLine(ex.Message);
                }
            });
            sp.Stop();
            _testOutputHelper.WriteLine($"{sp.ElapsedMilliseconds}");
        }
    }
}
