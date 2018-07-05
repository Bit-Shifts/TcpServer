using MySql.Data.MySqlClient;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace TcpServerAsync
{
    //Crude Tcp Server for testing throughput of IoT device data on different Network configuration and setups  
    public class TcpServer
    {
        object _lock = new Object(); // sync lock 
        List<Task> _connections = new List<Task>(); // pending connections
        private int _maxConnections = 2;

        public Task StartListener()
        {
            return Task.Run(async () =>
            {
                var tcpListener = TcpListener.Create(5000);
                tcpListener.Start();

                while (true)
                {
                    var tcpClient = await tcpListener.AcceptTcpClientAsync();
                    Console.WriteLine("[Server] Client has connected");
                    var task = StartHandleConnectionAsync(tcpClient);

                    if (task.IsFaulted)
                        task.Wait();
                }
            });
        }

        // Register and handle the connection
        private async Task StartHandleConnectionAsync(TcpClient tcpClient)
        {
            if (_connections.Count >= _maxConnections)
            {
                Console.WriteLine("Server Rejected Connection");
                tcpClient.Close();
            }
            else
            {
                var connectionTask = HandleConnectionAsync(tcpClient);

                lock (_lock)
                    _connections.Add(connectionTask);

                try
                {
                    await connectionTask;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                finally
                {
                    lock (_lock)
                        _connections.Remove(connectionTask);

                    tcpClient.Close();
                }
            }
        }

        // Handle new connection
        private static Task HandleConnectionAsync(TcpClient tcpClient)
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();

            return Task.Run(async () =>
            {
                BlockingCollection<byte[]> myQ = new BlockingCollection<byte[]>();
                bool isConnected = true;

                //An action to consume the ConcurrentQueue.
                Action dataHandler = () =>
                {
                    string myConnectionString = "server=localhost;database=tcptest;uid=root;pwd=Password";

                    List<string> rows = new List<string>();

                    while (!myQ.IsCompleted)
                    {
                        byte[] test = myQ.Take();

                        int i = BitConverter.ToInt32(test, 0);
                        int a = BitConverter.ToInt32(test, 4);
                        int b = BitConverter.ToInt32(test, 8);
                        long c = BitConverter.ToInt64(test, 12);

                        DateTime date = DateTime.FromBinary(c);

                        rows.Add(string.Format("('{0}','{1}','{2}','{3}')", MySqlHelper.EscapeString(i.ToString()), MySqlHelper.EscapeString(a.ToString()), MySqlHelper.EscapeString(b.ToString()), MySqlHelper.EscapeString(date.ToString("yyyy-MM-dd HH:MM:ss"))));

                        //Wait until 1000 records are ready to be inserted into the Datbase
                        if (rows.Count >= 1000)
                        {
                            bool successful = BulkToMySQL(myConnectionString, rows);
                            rows.Clear();
                        }
                    }
                };

                //Playing around with Actions and Task to monitor their behavoir in different instances
                var task = Task.Factory.StartNew(dataHandler, TaskCreationOptions.LongRunning);

                using (var networkStream = tcpClient.GetStream())
                {
                    while (isConnected)
                    {
                        //Message Header
                        byte[] sizeinfo = new byte[4];

                        //read the size of the message
                        int totalread = 0, currentread = 0;

                        currentread = totalread = await networkStream.ReadAsync(sizeinfo, 0, sizeinfo.Length);
                         
                        while (totalread < sizeinfo.Length && currentread > 0)
                        {
                            currentread = await networkStream.ReadAsync(sizeinfo, 0, sizeinfo.Length - totalread);  
                            totalread += currentread;
                        }

                        //Making sure connection is stil open if no bytes were recieved
                        if (totalread == 0)
                        {
                            if (!IsTcpClientConnected(tcpClient))
                            {
                                isConnected = false;
                                break;
                            }
                            else
                            {
                                continue;
                            }
                        }

                        //Move on to reading the message content
                        int messagesize = 0;

                        //could optionally call BitConverter.ToInt32(sizeinfo, 0);
                        messagesize = BitConverter.ToInt32(sizeinfo, 0);

                        //create a byte array of the correct size
                        byte[] data = new byte[messagesize];

                        //read the first chunk of data
                        totalread = 0;
                        currentread = totalread = await networkStream.ReadAsync(data, 0, data.Length);

                        //if we didn’t get the entire message, read some more until we do
                        while (totalread < messagesize && currentread > 0)
                        {
                            currentread = await networkStream.ReadAsync(data, 0, data.Length - totalread);
                            totalread += currentread;
                        }

                        //Making sure connection is still open if no bytes were recieved 
                        if (totalread == 0)
                        {
                            if (!IsTcpClientConnected(tcpClient))
                            {
                                isConnected = false;
                                break;
                            }
                            else
                            {
                                continue;
                            }
                        }

                        myQ.Add(data);
                    }

                    myQ.CompleteAdding();
                    task.Wait();

                    //Testing timing 
                    watch.Stop();

                    Console.WriteLine("[Server] Client dissconnected - Time: " + watch.Elapsed);
                }
            });
        }

        private static bool IsTcpClientConnected(TcpClient tcpClient)
        {
            bool isConnected = false;

            if (tcpClient.Client.Poll(0, SelectMode.SelectRead))
            {
                byte[] writeBuffer = new byte[1];

                if (tcpClient.Client.Receive(writeBuffer, SocketFlags.Peek) == 0)
                    isConnected = false;
                else
                    isConnected = true;
            }

            return isConnected;
        }

        public static bool BulkToMySQL(string myConnectionString, List<string> rows)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO test (Width, Height, SerialNumber, TimeStamp) VALUES ");

            using (MySqlConnection mConnection = new MySqlConnection(myConnectionString))
            {
                sCommand.Append(string.Join(",", rows));

                mConnection.Open();

                using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
                {
                    myCmd.CommandType = CommandType.Text;
                    myCmd.ExecuteNonQuery();
                }
            }

            return true;
        }
    }
}
