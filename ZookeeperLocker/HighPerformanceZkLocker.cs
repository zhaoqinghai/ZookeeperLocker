﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using ZookeeperLocker.Messaging;

namespace ZookeeperLocker
{
    /// <summary>
    /// 单例模式，ZkClient只有一次，不需要连接和断连的开销，增加性能。
    /// </summary>
    public static class ZkLockerManager
    {
        private static ZkOption _option;

        private static ZooKeeper Client;

        public static ZooKeeper GetClient()
        {
            if (!(Client.getState() == ZooKeeper.States.CONNECTED ||
                  Client.getState() == ZooKeeper.States.CONNECTEDREADONLY))
            {
                Client = CreateClient(_option);
            }

            return Client;
        }

        private static readonly object _lockObj = new object();

        public static void Configure(ZkOption option)
        {
            lock (_lockObj)
            {
                if (Client == null)
                {
                    _option = option;
                    Client = CreateClient(option);
                    Client.existsAsync("/locks").Wait();
                }
            }
        }

        private static ZooKeeper CreateClient(ZkOption option)
        {
            if(_option == null)
                throw new Exception("it is not initial");
            _option = option;
            return option.SessionId == long.MinValue
                ? new ZooKeeper(option.ConnectionsString, option.SessionTimeout, ZkWatcher.Default, option.CanBeReadOnly)
                : new ZooKeeper(option.ConnectionsString, option.SessionTimeout, ZkWatcher.Default, option.SessionId,
                    option.SessionPassword, option.CanBeReadOnly);
        }
        /// <summary>
        /// 获取锁
        /// </summary>
        /// <param name="lockName">锁的名称</param>
        /// <param name="timeout">超时时间</param>
        /// <returns></returns>
        public static IZkLocker GetLocker(string lockName, int timeout = 5000)
        {
            return new Locker(lockName, timeout);
        }
    }

    internal class ZkWatcher : Watcher
    {
        public static readonly ZkWatcher Default = new Lazy<ZkWatcher>(() => new ZkWatcher()).Value;

        private ZkWatcher() { }
        
        public override async Task process(WatchedEvent @event)
        {
            if (@event.get_Type() == Event.EventType.NodeDeleted)
            {
                var result = await ZkLockerManager.GetClient()
                    .getChildrenAsync(@event.getPath().Substring(0, @event.getPath().LastIndexOf('/')));
                var children = result.Children.OrderBy(item => int.Parse(Regex.Replace(item, @"[a-zA-Z|_]", "0"))).ToList();
                if (children.Count > 0)
                {
                    MessageManager<Null>.Publish(
                        @event.getPath().Substring(0, @event.getPath().LastIndexOf('/') + 1) + children[0], new Null());
                }
            }
        }
    }

    internal class Locker : IZkLocker
    {
        private readonly string _lockName;
        private readonly int _lockTimeout;
        private readonly EventWaitHandle _event = new ManualResetEvent(false);

        private string _currentNode;

        private readonly IObserver<Null> _observer;

        private void ReceiveMessage()
        {
            if (!_event.SafeWaitHandle.IsInvalid)
            {
                _event.Set();
            }
        }
        private async Task<bool> ExistPreNodeExecuted()
        {
            var result = await ZkLockerManager.GetClient().getChildrenAsync($"/locks/{_lockName}");
            var children = result.Children.OrderBy(item => int.Parse(Regex.Replace(item, @"[a-zA-Z|_]", "0"))).ToList();
            var currentIndex = children.IndexOf(_currentNode.Replace($"/locks/{_lockName}/", ""));
            if (currentIndex <= 0)
            {
                return false;
            }
            return true;
        }
        public Locker(string lockName, int timeout)
        {
            if (ZkLockerManager.GetClient().existsAsync("/locks").Result == null)
            {
                ZkLockerManager.GetClient().createAsync("/locks", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT).Wait();
            }
            if (ZkLockerManager.GetClient().existsAsync("/locks/" + lockName).Result == null)
            {
                ZkLockerManager.GetClient().createAsync("/locks/" + lockName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT).Wait();
            }
            _lockName = lockName;
            _lockTimeout = timeout;

            _observer = Observer.Create<Null>(_ => { ReceiveMessage(); });
        }

        public void Lock()
        {
            _currentNode = ZkLockerManager.GetClient().createAsync($"/locks/{_lockName}/node", new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL).Result;

            ZkLockerManager.GetClient().existsAsync(_currentNode, true);
           
            if (ExistPreNodeExecuted().Result)
            {
                MessageManager<Null>.Subscribe(_currentNode, _observer);
                var result = WaitHandle.WaitAny(new WaitHandle[] { _event }, TimeSpan.FromMilliseconds(_lockTimeout));
                if (result == WaitHandle.WaitTimeout || result == 1)
                {
                    UnLock();
                    throw new LockerTimeoutException();
                }
            }
        }

        public void UnLock()
        {
            ZkLockerManager.GetClient().deleteAsync(_currentNode).Wait();
            MessageManager<Null>.DeleteSubject(_currentNode);
            _observer.OnCompleted();
            _event.Dispose();
        }
    }

    public sealed class Null
    {
        
    }
}
