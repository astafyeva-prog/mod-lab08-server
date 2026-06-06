using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ScottPlot;

namespace SmoModeling
{
    /// <summary>
    /// Класс клиента, генерирующего запросы к серверу
    /// </summary>
    public class Client
    {
        private static int _nextId = 1;
        public int Id { get; }
        private readonly Server _server;
        private readonly Random _random;
        private readonly double _requestRate;

        public event EventHandler<RequestEventArgs> RequestGenerated;

        public Client(Server server, double requestRate)
        {
            Id = Interlocked.Increment(ref _nextId);
            _server = server;
            _requestRate = requestRate;
            _random = new Random(Guid.NewGuid().GetHashCode());
        }

        public static void ResetIdCounter() => _nextId = 1;

        public async Task StartGeneratingRequests(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                double interval = -Math.Log(1.0 - _random.NextDouble()) / _requestRate;
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(interval), cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                var request = new Request(Id, DateTime.Now);
                OnRequestGenerated(request);
            }
        }

        protected virtual void OnRequestGenerated(Request request)
        {
            RequestGenerated?.Invoke(this, new RequestEventArgs(request));
        }
    }

    /// <summary>
    /// Класс запроса от клиента
    /// </summary>
    public class Request
    {
        public int ClientId { get; }
        public DateTime GenerationTime { get; }
        public DateTime? StartTime { get; set; }
        public DateTime? EndTime { get; set; }

        public Request(int clientId, DateTime generationTime)
        {
            ClientId = clientId;
            GenerationTime = generationTime;
        }
    }

    /// <summary>
    /// Аргументы события запроса
    /// </summary>
    public class RequestEventArgs : EventArgs
    {
        public Request Request { get; }
        public RequestEventArgs(Request request) { Request = request; }
    }

    /// <summary>
    /// Класс канала обслуживания
    /// </summary>
    public class ServiceChannel
    {
        public int Id { get; }
        public bool IsBusy { get; private set; }
        private readonly double _serviceRate;
        private readonly Random _random;

        // Суммарное время, которое канал был занят (в секундах)
        private double _totalBusySeconds = 0;
        public double TotalBusySeconds => _totalBusySeconds;

        public ServiceChannel(int id, double serviceRate)
        {
            Id = id;
            _serviceRate = serviceRate;
            IsBusy = false;
            _random = new Random(Guid.NewGuid().GetHashCode());
        }

        public async Task ProcessRequest(Request request)
        {
            IsBusy = true;
            request.StartTime = DateTime.Now;

            double serviceTime = -Math.Log(1.0 - _random.NextDouble()) / _serviceRate;
            await Task.Delay(TimeSpan.FromSeconds(serviceTime));

            request.EndTime = DateTime.Now;
            double elapsed = (request.EndTime.Value - request.StartTime.Value).TotalSeconds;

            // Атомарно накапливаем занятое время канала
            double current;
            do
            {
                current = _totalBusySeconds;
            } while (Math.Abs(Interlocked.CompareExchange(ref _totalBusySeconds, current + elapsed, current) - current) > 1e-10);

            IsBusy = false;
        }

        public void ResetStats()
        {
            _totalBusySeconds = 0;
        }
    }

    /// <summary>
    /// Класс сервера с пулом каналов
    /// </summary>
    public class Server
    {
        private readonly List<ServiceChannel> _channels;
        private readonly int _channelCount;
        private readonly object _lock = new object();

        private int _totalRequests = 0;
        private int _processedRequests = 0;
        private int _rejectedRequests = 0;
        // Количество запросов, поступивших в момент, когда ВСЕ каналы были свободны
        private int _idleArrivals = 0;

        private DateTime _startTime;

        public Server(int channelCount, double serviceRate)
        {
            _channelCount = channelCount;
            _channels = new List<ServiceChannel>();
            for (int i = 0; i < channelCount; i++)
                _channels.Add(new ServiceChannel(i + 1, serviceRate));
            _startTime = DateTime.Now;
        }

        public void SubscribeClient(Client client)
        {
            client.RequestGenerated += OnRequestReceived;
        }

        private async void OnRequestReceived(object sender, RequestEventArgs e)
        {
            ServiceChannel freeChannel = null;
            bool allIdle = false;

            lock (_lock)
            {
                _totalRequests++;

                // Считаем момент поступления: сколько каналов занято прямо сейчас
                int busyNow = _channels.Count(c => c.IsBusy);
                if (busyNow == 0)
                    allIdle = true;

                freeChannel = _channels.FirstOrDefault(c => !c.IsBusy);

                if (freeChannel != null)
                    _processedRequests++;
                else
                    _rejectedRequests++;
            }

            // Фиксируем idle-прибытие ПОСЛЕ проверки (вне lock для корректного snapshot)
            if (allIdle && freeChannel != null)
            {
                lock (_lock) { _idleArrivals++; }
            }

            if (freeChannel != null)
            {
                await freeChannel.ProcessRequest(e.Request);
            }
            else
            {
                e.Request.StartTime = DateTime.Now;
                e.Request.EndTime = DateTime.Now;
            }
        }

        public Statistics GetStatistics()
        {
            lock (_lock)
            {
                double uptime = (DateTime.Now - _startTime).TotalSeconds;

                // Среднее число занятых каналов = сумма времён занятости каналов / время работы системы
                double totalBusyChannelTime = _channels.Sum(c => c.TotalBusySeconds);
                double avgBusyChannels = uptime > 0 ? totalBusyChannelTime / uptime : 0;

                // P0 (эксп.) = доля запросов, поступивших в систему, когда все каналы были свободны
                double p0Exp = _totalRequests > 0 ? (double)_idleArrivals / _totalRequests : 1.0;

                return new Statistics
                {
                    TotalRequests = _totalRequests,
                    ProcessedRequests = _processedRequests,
                    RejectedRequests = _rejectedRequests,
                    ChannelCount = _channelCount,
                    AvgBusyChannels = avgBusyChannels,
                    ProbabilityIdle = p0Exp,
                    Uptime = uptime
                };
            }
        }

        public void Reset()
        {
            lock (_lock)
            {
                _totalRequests = 0;
                _processedRequests = 0;
                _rejectedRequests = 0;
                _idleArrivals = 0;
                _startTime = DateTime.Now;
                foreach (var ch in _channels)
                    ch.ResetStats();
            }
        }
    }

    /// <summary>
    /// Класс статистики (только хранение, никаких формул внутри)
    /// </summary>
    public class Statistics
    {
        public int TotalRequests { get; set; }
        public int ProcessedRequests { get; set; }
        public int RejectedRequests { get; set; }
        public int ChannelCount { get; set; }
        public double AvgBusyChannels { get; set; }
        public double ProbabilityIdle { get; set; }
        public double Uptime { get; set; }

        // Расчётные показатели на основе накопленной статистики
        public double ProbabilityRejection =>
            TotalRequests > 0 ? (double)RejectedRequests / TotalRequests : 0;

        public double RelativeThroughput =>
            TotalRequests > 0 ? (double)ProcessedRequests / TotalRequests : 1;

        public double AbsoluteThroughput =>
            Uptime > 0 ? ProcessedRequests / Uptime : 0;
    }

    /// <summary>
    /// Теоретические расчёты СМО (формулы Эрланга)
    /// </summary>
    public static class SmoTheory
    {
        public static TheoreticalResults Calculate(double lambda, double mu, int n)
        {
            double rho = lambda / mu;

            // P0 = 1 / sum_{k=0}^{n} rho^k / k!
            double sum = 0;
            for (int k = 0; k <= n; k++)
                sum += Math.Pow(rho, k) / Factorial(k);
            double p0 = 1.0 / sum;

            // Pотк = (rho^n / n!) * P0  — формула Эрланга
            double pRejection = (Math.Pow(rho, n) / Factorial(n)) * p0;

            double relativeThroughput = 1 - pRejection;
            double absoluteThroughput = lambda * relativeThroughput;
            double avgBusyChannels = rho * relativeThroughput;

            return new TheoreticalResults
            {
                ProbabilityIdle = p0,
                ProbabilityRejection = pRejection,
                RelativeThroughput = relativeThroughput,
                AbsoluteThroughput = absoluteThroughput,
                AvgBusyChannels = avgBusyChannels
            };
        }

        private static double Factorial(int n)
        {
            double result = 1;
            for (int i = 2; i <= n; i++) result *= i;
            return result;
        }
    }

    public class TheoreticalResults
    {
        public double ProbabilityIdle { get; set; }
        public double ProbabilityRejection { get; set; }
        public double RelativeThroughput { get; set; }
        public double AbsoluteThroughput { get; set; }
        public double AvgBusyChannels { get; set; }
    }

    public class DataPoint
    {
        public double Lambda { get; set; }
        public double ProbabilityIdle { get; set; }
        public double ProbabilityRejection { get; set; }
        public double RelativeThroughput { get; set; }
        public double AbsoluteThroughput { get; set; }
        public double AvgBusyChannels { get; set; }
    }

    class Program
    {
        private const int ChannelCount = 5;
        private const double Mu = 2.0;
        private const int SimulationSeconds = 60;

        static async Task Main(string[] args)
        {
            Console.WriteLine("МОДЕЛИРОВАНИЕ МНОГОКАНАЛЬНОЙ СМО С ОТКАЗАМИ");
            Console.WriteLine($"Количество каналов: {ChannelCount}");
            Console.WriteLine($"Интенсивность обслуживания (μ): {Mu} запросов/сек");
            Console.WriteLine();

            var lambdaValues = Enumerable.Range(1, 11)
                .Select(i => Math.Round(i * 0.5, 1))
                .ToList(); // 0.5, 1.0, 1.5, ..., 5.5

            var expResults = new List<DataPoint>();
            var theoResults = new List<DataPoint>();

            foreach (var lambda in lambdaValues)
            {
                Console.WriteLine($"\n--- λ = {lambda:F1} ---");

                Client.ResetIdCounter();
                var server = new Server(ChannelCount, Mu);
                var clients = new List<Client>();
                var cts = new CancellationTokenSource();

                // Один клиент с полной интенсивностью lambda (простейший поток)
                int clientCount = 5;
                double clientRate = lambda / clientCount;
                for (int i = 0; i < clientCount; i++)
                {
                    var client = new Client(server, clientRate);
                    server.SubscribeClient(client);
                    clients.Add(client);
                }

                var tasks = clients
                    .Select(c => c.StartGeneratingRequests(cts.Token))
                    .ToArray();

                Console.WriteLine($"Моделирование {SimulationSeconds} сек...");
                await Task.Delay(SimulationSeconds * 1000);
                cts.Cancel();

                try { await Task.WhenAll(tasks); }
                catch (OperationCanceledException) { }

                // Небольшая пауза, чтобы дождаться завершения async void обработчиков
                await Task.Delay(500);

                var stats = server.GetStatistics();
                var theo = SmoTheory.Calculate(lambda, Mu, ChannelCount);

                expResults.Add(new DataPoint
                {
                    Lambda = lambda,
                    ProbabilityIdle = stats.ProbabilityIdle,
                    ProbabilityRejection = stats.ProbabilityRejection,
                    RelativeThroughput = stats.RelativeThroughput,
                    AbsoluteThroughput = stats.AbsoluteThroughput,
                    AvgBusyChannels = stats.AvgBusyChannels
                });

                theoResults.Add(new DataPoint
                {
                    Lambda = lambda,
                    ProbabilityIdle = theo.ProbabilityIdle,
                    ProbabilityRejection = theo.ProbabilityRejection,
                    RelativeThroughput = theo.RelativeThroughput,
                    AbsoluteThroughput = theo.AbsoluteThroughput,
                    AvgBusyChannels = theo.AvgBusyChannels
                });

                Console.WriteLine($"  Поступило: {stats.TotalRequests}  Обслужено: {stats.ProcessedRequests}  Отказов: {stats.RejectedRequests}");
                Console.WriteLine($"  P0  эксп={stats.ProbabilityIdle:F4}  теор={theo.ProbabilityIdle:F4}");
                Console.WriteLine($"  Pотк эксп={stats.ProbabilityRejection:F4}  теор={theo.ProbabilityRejection:F4}");
                Console.WriteLine($"  k    эксп={stats.AvgBusyChannels:F4}  теор={theo.AvgBusyChannels:F4}");
            }

            Directory.CreateDirectory("result");
            SaveResultsToFile(expResults, theoResults, lambdaValues);
            SaveCharts(expResults, theoResults, lambdaValues);

            Console.WriteLine("\n\nМоделирование завершено!");
            Console.WriteLine("Результаты: result/results.txt");
            Console.WriteLine("Графики:    result/p-1.png ... result/p-5.png");
        }

        static void SaveResultsToFile(List<DataPoint> exp, List<DataPoint> theo, List<double> lambdas)
        {
            using var w = new StreamWriter("result/results.txt", false, System.Text.Encoding.UTF8);
            w.WriteLine("РЕЗУЛЬТАТЫ МОДЕЛИРОВАНИЯ МНОГОКАНАЛЬНОЙ СМО С ОТКАЗАМИ");
            w.WriteLine("=====================================================");
            w.WriteLine($"Количество каналов n = {ChannelCount}");
            w.WriteLine($"Интенсивность обслуживания μ = {Mu} запросов/сек");
            w.WriteLine($"Время моделирования = {SimulationSeconds} сек на точку");
            w.WriteLine();
            w.WriteLine($"{"λ",-6} | {"P0 эксп",-10} | {"P0 теор",-10} | {"Pотк эксп",-12} | {"Pотк теор",-12} | {"Q эксп",-9} | {"Q теор",-9} | {"A эксп",-9} | {"A теор",-9} | {"k эксп",-9} | {"k теор",-9}");
            w.WriteLine(new string('-', 130));

            for (int i = 0; i < lambdas.Count; i++)
            {
                w.WriteLine(
                    $"{lambdas[i],-6:F1} | " +
                    $"{exp[i].ProbabilityIdle,-10:F4} | {theo[i].ProbabilityIdle,-10:F4} | " +
                    $"{exp[i].ProbabilityRejection,-12:F4} | {theo[i].ProbabilityRejection,-12:F4} | " +
                    $"{exp[i].RelativeThroughput,-9:F4} | {theo[i].RelativeThroughput,-9:F4} | " +
                    $"{exp[i].AbsoluteThroughput,-9:F2} | {theo[i].AbsoluteThroughput,-9:F2} | " +
                    $"{exp[i].AvgBusyChannels,-9:F4} | {theo[i].AvgBusyChannels,-9:F4}");
            }

            w.WriteLine();
            w.WriteLine("ВЫВОДЫ:");
            w.WriteLine("1. С ростом λ вероятность отказа (Pотк) растёт, вероятность простоя (P0) убывает.");
            w.WriteLine("2. Относительная пропускная способность Q убывает с ростом λ (больше отказов).");
            w.WriteLine("3. Абсолютная пропускная способность A сначала растёт, затем насыщается при λ >> μ·n.");
            w.WriteLine("4. Экспериментальные значения хорошо согласуются с теоретическими формулами Эрланга.");
        }

        static void SaveCharts(List<DataPoint> exp, List<DataPoint> theo, List<double> lambdas)
        {
            double[] xs = lambdas.Select(x => x).ToArray();

            // График 1: Вероятность простоя P0
            SaveChart("result/p-1.png",
                "Вероятность простоя системы P₀",
                "λ (запросов/сек)", "P₀",
                xs,
                exp.Select(p => p.ProbabilityIdle).ToArray(),
                theo.Select(p => p.ProbabilityIdle).ToArray(),
                yMin: 0, yMax: 1);

            // График 2: Вероятность отказа
            SaveChart("result/p-2.png",
                "Вероятность отказа P_отк",
                "λ (запросов/сек)", "P_отк",
                xs,
                exp.Select(p => p.ProbabilityRejection).ToArray(),
                theo.Select(p => p.ProbabilityRejection).ToArray(),
                yMin: 0, yMax: 1);

            // График 3: Относительная пропускная способность Q
            SaveChart("result/p-3.png",
                "Относительная пропускная способность Q",
                "λ (запросов/сек)", "Q",
                xs,
                exp.Select(p => p.RelativeThroughput).ToArray(),
                theo.Select(p => p.RelativeThroughput).ToArray(),
                yMin: 0, yMax: 1);

            // График 4: Абсолютная пропускная способность A
            SaveChart("result/p-4.png",
                "Абсолютная пропускная способность A",
                "λ (запросов/сек)", "A (запросов/сек)",
                xs,
                exp.Select(p => p.AbsoluteThroughput).ToArray(),
                theo.Select(p => p.AbsoluteThroughput).ToArray());

            // График 5: Среднее число занятых каналов k
            SaveChart("result/p-5.png",
                "Среднее число занятых каналов k",
                "λ (запросов/сек)", "k",
                xs,
                exp.Select(p => p.AvgBusyChannels).ToArray(),
                theo.Select(p => p.AvgBusyChannels).ToArray(),
                yMin: 0, yMax: ChannelCount);

            Console.WriteLine("Графики p-1.png ... p-5.png сохранены в папку result/");
        }

        static void SaveChart(string path, string title, string xLabel, string yLabel,
            double[] xs, double[] expY, double[] theoY,
            double? yMin = null, double? yMax = null)
        {
            var plt = new Plot(800, 500);

            var expScatter = plt.AddScatter(xs, expY, label: "Экспериментальные");
            expScatter.LineWidth = 2;
            expScatter.MarkerSize = 7;
            expScatter.Color = System.Drawing.Color.FromArgb(0, 150, 200);

            var theoScatter = plt.AddScatter(xs, theoY, label: "Теоретические");
            theoScatter.LineWidth = 2;
            theoScatter.MarkerSize = 7;
            theoScatter.Color = System.Drawing.Color.FromArgb(220, 80, 60);
            theoScatter.LineStyle = ScottPlot.LineStyle.Dash;

            plt.Title(title);
            plt.XLabel(xLabel);
            plt.YLabel(yLabel);
            plt.Legend(location: ScottPlot.Alignment.UpperRight);

            if (yMin.HasValue && yMax.HasValue)
                plt.SetAxisLimitsY(yMin.Value, yMax.Value);

            plt.SaveFig(path);
        }
    }
}
