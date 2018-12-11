using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reactive.Linq;

namespace ZookeeperLocker.Messaging
{
    public static class MessageManager<T>
    {
        /// <summary>
        /// 缓存消息与主题
        /// </summary>
        private static ConcurrentDictionary<string, WeakReference<IObserver<T>>> _cache =
            new ConcurrentDictionary<string, WeakReference<IObserver<T>>>();

        /// <summary>
        /// 订阅特定主题的消息
        /// </summary>
        /// <typeparam name="T">消息体</typeparam>
        /// <param name="subject">主题名称</param>
        /// <param name="observer">消息体的监听者</param>
        public static void Subscribe(string subject, IObserver<T> observer)
        {
            var item = _cache.AddOrUpdate(subject, new WeakReference<IObserver<T>>(observer),
                (@key, @value) => new WeakReference<IObserver<T>>(observer));
        }

        /// <summary>
        /// 发布特定主题的消息
        /// </summary>
        /// <param name="subject"></param>
        /// <param name="message"></param>
        public static void Publish(string subject, T message)
        {
            if (_cache.TryGetValue(subject, out var value))
            {
                if (value.TryGetTarget(out var target))
                {
                    target.OnNext(message);
                }
            }

        }

        /// <summary>
        /// 删除主题 取消该主题的发布
        /// </summary>
        /// <param name="subject"></param>
        public static void DeleteSubject(string subject)
        {
            WeakReference<IObserver<T>> value;
            _cache.TryRemove(subject, out value);
        }

        /// <summary>
        /// 定时处理无效的缓存 防止泄露
        /// </summary>
        static MessageManager()
        {
            Observable.Timer(DateTimeOffset.Now, TimeSpan.FromMinutes(5)).Subscribe(_ =>
            {
                var tempCache = _cache.ToArray();
                var invalidCache = tempCache.Where(item =>
                {
                    if (!item.Value.TryGetTarget(out var target))
                    {
                        return true;
                    }

                    return target == null;
                });
                foreach (var keyValuePair in invalidCache)
                {
                    _cache.TryRemove(keyValuePair.Key, out var _);
                }
            });
        }

    }
}