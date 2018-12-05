using System;
using System.Collections.Generic;
using System.Text;

namespace ZookeeperLocker
{
    public class ZkOption
    {
        public string ConnectionsString { get; set; }

        public int SessionTimeout { get; set; }

        public long SessionId { get; set; } = long.MinValue;

        public byte[] SessionPassword { get; set; } = default(byte[]) ;

        public bool CanBeReadOnly { get; set; } = false;
    }
}
