using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

class RequestEventArgs : EventArgs
{
    public int RequestId { get; set; }

    public RequestEventArgs(int id)
    {
        RequestId = id;
    }
}

class Client
{
    public event EventHandler<RequestEventArgs>? RequestGenerated;

    private readonly Random random = new Random();

    public async Task GenerateRequests(int count, int delay)
    {
        for (int i = 1; i <= count; i++)
        {
            RequestGenerated?.Invoke(
                this,
                new RequestEventArgs(i));

            await Task.Delay(
                random.Next(delay / 2, delay));
        }
    }
}

class Server
{
    private readonly int channelCount;

    private int busyChannels = 0;

    public int TotalRequests { get; private set; }

    public int AcceptedRequests { get; private set; }

    public int RejectedRequests { get; private set; }

    public Server(int channels)
    {
        channelCount = channels;
    }

    public async void HandleRequest(
        object? sender,
        RequestEventArgs e)
    {
        TotalRequests++;

        if (busyChannels >= channelCount)
        {
            RejectedRequests++;

            Console.WriteLine(
                $"Request {e.RequestId} rejected");

            return;
        }

        busyChannels++;

        AcceptedRequests++;

        Console.WriteLine(
            $"Request {e.RequestId} accepted");

        await Task.Run(async () =>
        {
            await Task.Delay(1000);
        });

        busyChannels--;
    }

    public void PrintStatistics()
    {
        Console.WriteLine();
        Console.WriteLine(
            $"Total requests: {TotalRequests}");

        Console.WriteLine(
            $"Accepted requests: {AcceptedRequests}");

        Console.WriteLine(
            $"Rejected requests: {RejectedRequests}");

        double rejectProbability =
            (double)RejectedRequests / TotalRequests;

        double throughput =
            (double)AcceptedRequests / TotalRequests;

        Console.WriteLine(
            $"Reject probability: {rejectProbability:F2}");

        Console.WriteLine(
            $"Relative throughput: {throughput:F2}");
    }
}

class Program
{
    static async Task Main()
    {
        Server server = new Server(3);

        Client client = new Client();

        client.RequestGenerated +=
            server.HandleRequest;

        await client.GenerateRequests(
            20,
            300);

        await Task.Delay(3000);

        server.PrintStatistics();
    }
}
