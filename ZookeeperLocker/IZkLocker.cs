using System;
using System.Collections.Generic;
using System.Text;

namespace ZookeeperLocker
{
    public interface IZkLocker
    {
        void Lock();

        void UnLock();
    }
}
