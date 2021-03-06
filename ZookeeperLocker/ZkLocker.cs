﻿using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using org.apache.zookeeper;

namespace ZookeeperLocker
{
    public class ZkLocker : Watcher, IZkLocker
    {
        private readonly ZooKeeper _client;

        private string _currentNode;

        private readonly EventWaitHandle _event = new ManualResetEvent(false);

        private readonly string _lockName;

        private readonly int _lockTimeout;

        /// <summary>
        /// 全局默认锁defaultLock
        /// </summary>
        /// <param name="option">配置选项</param>
        public ZkLocker(ZkOption option) : this("defaultLock", option)
        {

        }

        /// <summary>
        /// 全局默认锁defaultLock
        /// </summary>
        /// <param name="option">配置选项</param>
        /// <param name="lockTimeout">设置锁超时时间</param>
        public ZkLocker(ZkOption option,int lockTimeout) : this("defaultLock", option, lockTimeout)
        {

        }
        public ZkLocker(string lockName, ZkOption option, int lockTimeout = 5000)
        {
            _client = option.SessionId == long.MinValue
                ? new ZooKeeper(option.ConnectionsString, option.SessionTimeout, this, option.CanBeReadOnly)
                : new ZooKeeper(option.ConnectionsString, option.SessionTimeout, this, option.SessionId,
                    option.SessionPassword, option.CanBeReadOnly);
            if (_client.existsAsync("/locks").Result == null)
            {
                _client.createAsync("/locks", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT).Wait();
            }
            if (_client.existsAsync("/locks/" + lockName).Result == null)
            {
                _client.createAsync("/locks/" + lockName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT).Wait();
            }
            _lockName = lockName;
            _lockTimeout = lockTimeout;
        }

        /// <summary>
        /// lock
        /// </summary>
        public void Lock()
        {
            _currentNode = _client.createAsync($"/locks/{_lockName}/node", new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL).Result;

            if (ExistPreNodeExecuted().Result)
            {
                var result = WaitHandle.WaitAny(new WaitHandle[] { _event }, TimeSpan.FromMilliseconds(_lockTimeout));
                if (result == WaitHandle.WaitTimeout || result == 1)
                {
                    UnLock();
                    throw new LockerTimeoutException();
                }
            }
        }

        private async Task<bool> ExistPreNodeExecuted()
        {
            var result = await _client.getChildrenAsync($"/locks/{_lockName}");
            var children = result.Children.OrderBy(item => int.Parse(Regex.Replace(item, @"[a-zA-Z|_]", "0"))).ToList();
            var currentIndex = children.IndexOf(_currentNode.Replace($"/locks/{_lockName}/", ""));
            if (currentIndex <= 0)
            {
                return false;
            }
            var preNode = await _client.existsAsync($"/locks/{_lockName}/" + children[currentIndex - 1], true);
            if (preNode == null)
            {
                return await ExistPreNodeExecuted();
            }

            return true;
        }

        public void UnLock()
        {
            _client.deleteAsync(_currentNode).Wait();
            _client.closeAsync().Wait();
            _event.Dispose();
        }

        public override async Task process(WatchedEvent @event)
        {
            if (@event.get_Type() == Event.EventType.NodeDeleted)
            {
                if (!await ExistPreNodeExecuted())
                {
                    if (!_event.SafeWaitHandle.IsClosed)
                    {
                        _event.Set();
                    }
                }
            }
        }
    }


}
