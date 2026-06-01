using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Lab08
{
    class RequestEventArgs : EventArgs
    {
        public int Id { get; set; }
    }

    class Client
    {
        public event EventHandler<RequestEventArgs>? RequestGenerated;

        private int _requestId = 0;

        public void GenerateRequest()
        {
            RequestGenerated?.Invoke(this,
                new RequestEventArgs { Id = ++_requestId });
        }
    }

    class Server
    {
        private readonly int _channels;
        private readonly double _serviceTimeMs;

        private int _busyChannels = 0;

        public int TotalRequests { get; private set; }
        public int AcceptedRequests { get; private set; }
        public int RejectedRequests { get; private set; }

        public long BusyChannelsSum { get; private set; }
        public long Measurements { get; private set; }

        public Server(int channels, double serviceTimeMs)
        {
            _channels = channels;
            _serviceTimeMs = serviceTimeMs;
        }

        public void Connect(Client client)
        {
            client.RequestGenerated += ProcessRequest;
        }

        private void ProcessRequest(object? sender, RequestEventArgs e)
        {
            Interlocked.Increment(ref TotalRequests);

            lock (this)
            {
                if (_busyChannels >= _channels)
                {
                    RejectedRequests++;
                    return;
                }

                _busyChannels++;
                AcceptedRequests++;
            }

            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay((int)_serviceTimeMs);
                }
                finally
                {
                    lock (this)
                    {
                        _busyChannels--;
                    }
                }
            });
        }

        public void Measure()
        {
            lock (this)
            {
                BusyChannelsSum += _busyChannels;
                Measurements++;
            }
        }

        public double AverageBusyChannels =>
            Measurements == 0
                ? 0
                : (double)BusyChannelsSum / Measurements;
    }

    class Program
    {
        static async Task Main()
        {
            int n = 4;

            double mu = 2.0;

            Console.WriteLine("Lambda\tPотк");

            for (double lambda = 0.5; lambda <= 5.0; lambda += 0.5)
            {
                var server = new Server(
                    n,
                    1000.0 / mu);

                var client = new Client();

                server.Connect(client);

                int simulationTimeMs = 30000;

                DateTime finish =
                    DateTime.Now.AddMilliseconds(simulationTimeMs);

                while (DateTime.Now < finish)
                {
                    client.GenerateRequest();

                    server.Measure();

                    await Task.Delay(
                        (int)(1000.0 / lambda));
                }

                await Task.Delay(3000);

                double rejectProbability =
                    (double)server.RejectedRequests /
                    server.TotalRequests;

                Console.WriteLine(
                    $"{lambda:F1}\t{rejectProbability:F4}");
            }
        }
    }
}
