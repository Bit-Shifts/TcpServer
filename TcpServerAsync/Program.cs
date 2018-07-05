using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TcpServerAsync
{
    class Program
    {
        static void Main(string[] args)
        {
            Task.Run(() => { new TcpServer().StartListener().Wait(); });
        }
    }
}
