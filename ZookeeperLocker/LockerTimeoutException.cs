using System;
using System.Collections.Generic;
using System.Text;

namespace ZookeeperLocker
{
    public class LockerTimeoutException : Exception
    {
        public LockerTimeoutException() : base("locker time out")
        {

        }
    }
}
