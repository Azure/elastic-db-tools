// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Purpose:
// Network proxy useful for simulating network failures, connection failures, etc.
//
// Notes:
// This source code copied from:

using System;
using System.Collections.Generic;
using System.Data;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;


namespace Microsoft.Azure.SqlDatabase.ElasticScale.Query.UnitTests
{
    /// <summary>
    /// This is a simple network listener that is redirects the traffic
    /// It is used to simulate network delay
    /// </summary>
    public class ProxyServer : IDisposable
    {
        private volatile bool _stopRequested;
        private StringBuilder _eventLog = new StringBuilder(s_logHeader, 1000);
        private static string s_logHeader = "======== ProxyServer Log Start ========\n";
        private static string s_logTrailer = "======== ProxyServer Log End ========\n";

        /// <summary>
        /// Gets the event log for the proxy server
        /// </summary>
        internal StringBuilder EventLog
        {
            get
            {
                return _eventLog;
            }
        }

        /// <summary>
        /// SleepResetEvent to cancel proxy server sleep delay
        /// </summary>
        internal ManualResetEventSlim SleepResetEvent = new ManualResetEventSlim();

        /// <summary>
        /// The list of connections spawned by the server
        /// </summary>
        internal IList<ProxyServerConnection> Connections { get; private set; }

        /// <summary>
        /// Synchronization object on the list
        /// </summary>
        internal object SyncRoot { get; private set; }

        /// <summary>
        /// Gets the local port that is being listened on
        /// </summary>
        public int LocalPort { get; private set; }

        /// <summary>
        /// Gets/Sets the remote end point to connect to
        /// </summary>
        public IPEndPoint RemoteEndpoint { get; set; }

        /// <summary>
        /// Gets/Sets the listener
        /// </summary>
        protected TcpListener ListenerSocket { get; set; }

        /// <summary>
        /// Gets/Sets the listener thread
        /// </summary>
        protected Thread ListenerThread { get; set; }

        /// <summary>
        /// Delay incoming 
        /// </summary>
        public bool SimulatedInDelay { get; set; }

        /// <summary>
        /// Delay outgoing
        /// </summary>
        public bool SimulatedOutDelay { get; set; }

        /// <summary>
        /// Simulated delay in milliseconds between each packet being written out. This simulates low bandwidth connection.
        public int SimulatedPacketDelay { get; private set; }

        /// <summary>
        /// Size of Buffer
        /// </summary>
        public int BufferSize { get; set; }

        /// <summary>
        /// Gets/Sets the flag whether the stop is requested
        /// </summary>
        internal bool StopRequested { get { return _stopRequested; } set { _stopRequested = value; } }

        /// <summary>
        /// Set SimulatedPacketDelay
        /// </summary>
        public void SetSimulatedPacketDelay(int simulatedPacketDelay)
        {
            Log("Setting SimulatedPacketDelay to: {0}", simulatedPacketDelay);
            SimulatedPacketDelay = simulatedPacketDelay;
            SleepResetEvent.Reset();
        }

        /// <summary>
        /// Reset SimulatedPacketDelay
        /// </summary>
        public void ResetSimulatedPacketDelay()
        {
            Log("Resetting SimulatedPacketDelay to: 0");
            SimulatedPacketDelay = 0;
            SleepResetEvent.Set();
        }

        /// <summary>
        /// Default constructor
        /// </summary>
        public ProxyServer(int simulatedPacketDelay = 0, bool simulatedInDelay = false, bool simulatedOutDelay = false, int bufferSize = 8192)
        {
            SyncRoot = new object();
            Connections = new List<ProxyServerConnection>();
            SimulatedPacketDelay = simulatedPacketDelay;
            SimulatedInDelay = simulatedInDelay;
            SimulatedOutDelay = simulatedOutDelay;
            BufferSize = bufferSize;
        }

        /// <summary>
        /// Start the listener thread
        /// </summary>
        public void Start()
        {
            StopRequested = false;

            Log("Starting the server...");

            // Listen on any port
            ListenerSocket = new TcpListener(new IPEndPoint(IPAddress.Any, 0));
            ListenerSocket.Start();

            Log("Server is running on {0}...", ListenerSocket.LocalEndpoint);

            LocalPort = ((IPEndPoint)ListenerSocket.LocalEndpoint).Port;

            ListenerThread = new Thread(new ThreadStart(_RequestListener));
            ListenerThread.Name = "Proxy Server Listener";
            ListenerThread.Start();
        }

        /// <summary>
        /// Stop the listener thread
        /// </summary>
        public void Stop()
        {
            // Request the listener thread to stop
            StopRequested = true;

            // Wait for termination
            ListenerThread.Join(1000);
        }

        public void Dispose()
        {
            Stop();
        }

        /// <summary>
        /// This method is used internally to notify server about the client disconnection
        /// </summary>
        internal void NotifyClientDisconnection(ProxyServerConnection connection)
        {
            lock (SyncRoot)
            {
                // Remove the existing connection from the list
                Connections.Remove(connection);
            }
        }

        /// <summary>
        /// Processes all incoming requests
        /// </summary>
        private void _RequestListener()
        {
            try
            {
                while (!StopRequested)
                {
                    if (ListenerSocket.Pending())
                    {
                        try
                        {
                            Log("Connection received");

                            // Accept the connection
                            TcpClient newConnection = ListenerSocket.AcceptTcpClient();

                            //Log("Connection accepted");

                            // Start a new connection
                            ProxyServerConnection proxyConnection = new ProxyServerConnection(newConnection, this);
                            proxyConnection.Start();

                            // Registering new connection
                            lock (SyncRoot)
                            {
                                Connections.Add(proxyConnection);
                            }
                        }
                        catch (Exception ex)
                        {
                            Log(ex.ToString());
                        }
                    }
                    else
                    {
                        // Pause a bit
                        Thread.Sleep(10);
                    }
                }
            }
            catch (Exception ex)
            {
                Log(ex.ToString());
            }

            Log("Stopping the server...");

            // Stop the server
            ListenerSocket.Stop();
            ListenerSocket = null;

            Log("Waiting for client connections to terminate...");

            // Wait for connections
            int connectionsLeft = Int16.MaxValue;
            while (connectionsLeft > 0)
            {
                lock (SyncRoot)
                {
                    // Check the amount of connections left
                    connectionsLeft = Connections.Count;
                }

                // Wait a bit
                Thread.Sleep(10);
            }

            Log("Server is stopped");
        }


        /// <summary>
        /// Write a string to the log
        /// </summary>
        internal void Log(string text, params object[] args)
        {
            lock (EventLog)
            {
                EventLog.AppendFormat("[{0:O}]: ", DateTime.Now);
                EventLog.AppendFormat(text, args);
                EventLog.AppendLine();
            }
        }

        /// <summary>
        /// Return the ProxyServer log
        /// </summary>
        /// <returns></returns>
        public string GetServerEventLog()
        {
            lock (EventLog)
            {
                EventLog.Append(s_logTrailer);
                return EventLog.ToString();
            }
        }

        /// <summary>
        /// Get Server IPEndPoint
        /// </summary>
        public static IPEndPoint GetServerIpEndpoint(IDbConnection conn)
        {
            using (IDbCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = "select local_net_address, local_tcp_port " +
                                  "from [sys].[dm_exec_connections] " +
                                  "where local_net_address is not null and local_tcp_port is not null and session_id = @@spid";
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        string ipAddress = reader.GetString(0);
                        int port = reader.GetInt32(1);
                        return new IPEndPoint(IPAddress.Parse(ipAddress), port);
                    }
                    else
                    {
                        // No rows are non-null, so assume it is local
                        // Assume that we're listening on the default port
                        return new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1433);
                    }
                }
            }
        }

        /// <summary>
        /// Kills all currently open connections
        /// </summary>
        /// <param name="softKill">If true will perform a shutdown before closing, otherwise close will happen with lingering disabled</param>
        public void KillAllConnections(bool softKill = false)
        {
            lock (SyncRoot)
            {
                foreach (var connection in Connections)
                {
                    connection.Kill(softKill);
                }
            }
        }
    }

    /// <summary>
    /// This class maintains the tunnel between incoming connection and outgoing connection
    /// </summary>
    internal class ProxyServerConnection
    {
        /// <summary>
        /// This is a processing thread
        /// </summary>
        protected Thread ProcessorThread { get; set; }

        /// <summary>
        /// Returns the proxy server this connection belongs to
        /// </summary>
        public ProxyServer Server { get; set; }

        /// <summary>
        /// Incoming connection
        /// </summary>
        protected TcpClient IncomingConnection { get; set; }

        /// <summary>
        /// Outgoing connection
        /// </summary>
        protected TcpClient OutgoingConnection { get; set; }

        /// <summary>
        /// Standard constructor
        /// </summary>
        public ProxyServerConnection(TcpClient incomingConnection, ProxyServer server)
        {
            IncomingConnection = incomingConnection;
            Server = server;
        }

        /// <summary>
        /// Runs the processing thread
        /// </summary>
        public void Start()
        {
            ProcessorThread = new Thread(new ThreadStart(_ProcessorHandler));

            IPEndPoint incomingIPEndPoint = IncomingConnection.Client.RemoteEndPoint as IPEndPoint;
            ProcessorThread.Name = string.Format("Proxy Server Connection {0} Thread", incomingIPEndPoint);

            ProcessorThread.Start();
        }

        /// <summary>
        /// Handles the bidirectional data transfers
        /// </summary>
        private void _ProcessorHandler()
        {
            try
            {
                Server.Log("Connecting to {0}...", Server.RemoteEndpoint);
                Server.Log("Remote end point address family {0}", Server.RemoteEndpoint.AddressFamily);

                // Establish outgoing connection to the proxy
                OutgoingConnection = new TcpClient(Server.RemoteEndpoint.AddressFamily);
                OutgoingConnection.Connect(Server.RemoteEndpoint);

                // Writing connection information
                Server.Log("Connection established");

                // Obtain network streams
                NetworkStream outStream = OutgoingConnection.GetStream();
                NetworkStream inStream = IncomingConnection.GetStream();

                // Tunnel the traffic between two connections
                while (IncomingConnection.Connected && OutgoingConnection.Connected && !Server.StopRequested)
                {
                    // Check incoming buffer
                    if (inStream.DataAvailable)
                    {
                        CopyData(inStream, "client", outStream, "server", Server.SimulatedInDelay);
                    }

                    // Check outgoing buffer
                    if (outStream.DataAvailable)
                    {
                        CopyData(outStream, "server", inStream, "client", Server.SimulatedOutDelay);
                    }

                    // Poll the sockets
                    if ((IncomingConnection.Client.Poll(100, SelectMode.SelectRead) && !inStream.DataAvailable) ||
                        (OutgoingConnection.Client.Poll(100, SelectMode.SelectRead) && !outStream.DataAvailable))
                        break;

                    Thread.Sleep(10);
                }
            }
            catch (Exception ex)
            {
                Server.Log(ex.ToString());
            }

            try
            {
                // Disconnect the client
                IncomingConnection.Close();
                OutgoingConnection.Close();
            }
            catch (Exception) { }

            // Logging disconnection message
            Server.Log("Connection closed");

            // Notify parent
            Server.NotifyClientDisconnection(this);
        }

        private void CopyData(NetworkStream readStream, string readStreamName, NetworkStream writeStream, string writeStreamName, bool simulateDelay)
        {
            // Note that the latency/bandwidth delay algorithm used here is a simple approximation used for test purposes

            Server.Log("Copying message from {0} to {1}, delay is {2}", readStreamName, writeStreamName, simulateDelay);

            // Read all available data from readStream
            var outBytes = new List<Tuple<byte[], int>>();
            while (readStream.DataAvailable)
            {
                byte[] buffer = new byte[Server.BufferSize];
                int numBytes = readStream.Read(buffer, 0, buffer.Length);
                outBytes.Add(new Tuple<byte[], int>(buffer, numBytes));
                Server.Log("\tRead {0} bytes from {1}", numBytes, readStreamName);
            }


            // Write all data to writeStream
            foreach (var b in outBytes)
            {
                // Delay for bandwidth, only do the simulated packet delay if SleepResetEvent hasn't been set
                if (simulateDelay && !Server.SleepResetEvent.IsSet)
                {
                    Server.Log("\tSleeping packet delay for {0}", Server.SimulatedPacketDelay);
                    Server.SleepResetEvent.Wait(Server.SimulatedPacketDelay);
                }

                writeStream.Write(b.Item1, 0, b.Item2);
                Server.Log("\tWrote {0} bytes to {1}", b.Item2, writeStreamName);
            }
        }

        /// <summary>
        /// Kills this connection
        /// </summary>
        /// <param name="softKill">If true will perform a shutdown before closing, otherwise close will happen with lingering disabled</param>
        /// <remarks>
        /// See VSTS item 2672651.
        /// We've been encountering an issue in our lab dttp runs in which we hit an ObjectDisposed exception in the call to 
        /// IncomingConnection.Client.Shutdown.  Per the msdn docs on that call, the ObjectDisposedException happens if the
        /// Socket has already been closed.  Since the point of this method is to close the socket, we'll just catch the
        /// ObjectDisposedException and ignore it.  
        /// Note that the LingerState property can also throw an ObjectDisposedException if the Socket has already been
        /// closed, so we'll put the try-catch block around both code paths.
        /// Finally, note that the docuemntation for the Close call does not list an ObjectDisposedException as something that
        /// can be thrown if the Socket has already been closed, so we will leave those calls out of the try-catch block.
        /// If we start seeing ObjectDisposedExcptions coming from there then we can investigate further.  For now the goal
        /// is to be conservative in the error handling case so that we don't inadvertantly ignore exceptions that are happening
        /// for a different reason.
        /// </remarks>
        public void Kill(bool softKill)
        {
            Server.Log("About to kill a client connection.");
            try
            {
                if (softKill)
                {
                    // Soft close, do shutdown first
                    IncomingConnection.Client.Shutdown(SocketShutdown.Both);
                    Server.Log("Successfully issued a soft kill to client connection.");
                }
                else
                {
                    // Hard close - force no lingering
                    IncomingConnection.Client.LingerState = new LingerOption(true, 0);
                    Server.Log("Successfully issued a hard kill to client connection.");
                }
            }
            catch (ObjectDisposedException ode)
            {
                Server.Log("Ignoring ObjectDisposedException while preparing to close a client connection.");
                Server.Log("Exception info:\n\t Message: {0} \n\t ObjectName: {1} \n\t Source: {2} \n\t StackTrace: {3}",
                    ode.Message, ode.ObjectName, ode.Source, ode.StackTrace);
            }

            IncomingConnection.Client.Close();
            Server.Log("Successfully closed incoming connection.");
            OutgoingConnection.Client.Close();
            Server.Log("Successfully closed outgoing connection.");
        }
    }
}
