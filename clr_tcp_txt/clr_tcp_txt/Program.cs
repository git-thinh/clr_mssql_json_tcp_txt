using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace clr_tcp_txt
{
    class Program
    {
        //const int CHUNK_SIZE = 65535;
        const int CHUNK_SIZE = 1 * 1024 * 1024; //1MB
        const int _1MB = 1024 * 1024; //1MB

        static string title = "";

        static void Main(string[] args)
        {
            string _ip = "127.0.0.1";
            int _port = 6969;

            if (!Directory.Exists("txt")) Directory.CreateDirectory("txt");

            try
            {
                IPAddress ipAddress = IPAddress.Parse(_ip);
                TcpListener listener = new TcpListener(ipAddress, _port);

                title = string.Format("TCP {0}:{1}", _ip, _port);
                Console.Title = title;

                listener.Start();
                while (true)
                {
                    Socket client = listener.AcceptSocket();

                    var childSocketThread = new Thread(new ParameterizedThreadStart((o) =>
                    {
                        int total = 0;
                        int k = 0;
                        int len = 0;
                        byte[] buffer = new byte[CHUNK_SIZE];
                        var socket = (Socket)o;

                        string file_name = "";

                        using (var stream = new MemoryStream())
                        {
                            byte[] actual;

                            Stopwatch stopwatch = new Stopwatch();
                            stopwatch.Start();

                            while ((len = socket.Receive(buffer, buffer.Length, SocketFlags.None)) > 0)
                            {
                                total += len;

                                if (k == 0)
                                {
                                    if (len < 255)
                                    {
                                        Console.WriteLine("Buffer invalid ...");
                                        break;
                                    }
                                    file_name = Encoding.ASCII.GetString(buffer, 0, 255).Trim().ToUpper();

                                    Console.WriteLine("-> " + file_name + " buffering ...");

                                    actual = new byte[len - 255];
                                    Buffer.BlockCopy(buffer, 255, actual, 0, len - 255);
                                }
                                else
                                {
                                    actual = new byte[len];
                                    Buffer.BlockCopy(buffer, 0, actual, 0, len);
                                }

                                if (k % 5 == 0) Console.Title = String.Format("{0:N0} MB", total / _1MB);

                                stream.Write(actual, 0, actual.Length);
                                k++;
                            }


                            if (file_name.Length > 0)
                            {
                                string file = "txt\\" + file_name + ".txt";
                                if (File.Exists(file)) File.Delete(file);

                                //var fileStream = File.Create(file);
                                //stream.Seek(0, SeekOrigin.Begin);
                                //stream.CopyTo(fileStream);
                                //fileStream.Close();

                                int fileSize = (int)stream.Length;
                                using (FileStream fileStream = new FileStream(file, FileMode.Create, System.IO.FileAccess.Write))
                                {
                                    stream.Seek(0, SeekOrigin.Begin);
                                    byte[] bytes = new byte[fileSize];
                                    stream.Read(bytes, 0, fileSize);
                                    fileStream.Write(bytes, 0, fileSize);
                                    stream.Close();
                                }
                            }

                            stopwatch.Stop();
                            TimeSpan ts = stopwatch.Elapsed;
                            string elapsedTime = String.Format(" | Duration: {0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
                            string s = string.Format("-> {0} OK = {1}", file_name, String.Format("{0:N0} MB", total / _1MB)) + elapsedTime;
                            Console.Title = string.Format("{0} -> {1}", title, s);
                            Console.WriteLine(s);
                        }

                        socket.Close();
                    }
                    ));
                    childSocketThread.Start(client);
                }

                listener.Stop();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.StackTrace);
                Console.ReadLine();
            }
        }

    }
}
