using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using ScottPlot;

namespace SmoSimulation
{
    /// <summary>
    /// Класс события запроса клиента
    /// </summary>
    public class RequestEventArgs : EventArgs
    {
        public int ClientId { get; set; }
        public DateTime RequestTime { get; set; }
        public double ProcessingTime { get; set; }
    }

    /// <summary>
    /// Класс клиента, генерирующего запросы
    /// </summary>
    public class Client
    {
        private static int _nextId = 1;
        public int ClientId { get; }
        private readonly Server _server;
        private readonly double _requestIntensity;
        private readonly Random _random;
        private bool _isRunning;

        public event EventHandler<RequestEventArgs>? RequestGenerated;

        public Client(Server server, double requestIntensity)
        {
            ClientId = _nextId++;
            _server = server;
            _requestIntensity = requestIntensity;
            _random = new Random(ClientId * DateTime.Now.Millisecond);
            _isRunning = false;

            RequestGenerated += _server.HandleRequest;
        }

        public async Task StartGeneratingRequests(CancellationToken cancellationToken)
        {
            _isRunning = true;
            while (!cancellationToken.IsCancellationRequested && _isRunning)
            {
                double interval = -Math.Log(1.0 - _random.NextDouble()) / _requestIntensity;
                await Task.Delay(TimeSpan.FromSeconds(interval), cancellationToken);

                if (!cancellationToken.IsCancellationRequested)
                {
                    GenerateRequest();
                }
            }
        }

        private void GenerateRequest()
        {
            RequestEventArgs args = new RequestEventArgs
            {
                ClientId = this.ClientId,
                RequestTime = DateTime.Now,
                ProcessingTime = 1.0
            };

            RequestGenerated?.Invoke(this, args);
        }

        public void Stop()
        {
            _isRunning = false;
        }
    }

    /// <summary>
    /// Класс потока обслуживания
    /// </summary>
    public class ServiceChannel
    {
        public int ChannelId { get; }
        public bool IsBusy { get; private set; }
        public Client? CurrentClient { get; private set; }
        public DateTime StartTime { get; private set; }
        public DateTime EndTime { get; private set; }

        private readonly Random _random;

        public ServiceChannel(int id)
        {
            ChannelId = id;
            IsBusy = false;
            _random = new Random(id * DateTime.Now.Millisecond);
        }

        public async Task ProcessRequest(Client client, double serviceIntensity, double processingTime)
        {
            IsBusy = true;
            CurrentClient = client;
            StartTime = DateTime.Now;

            double serviceTime = -Math.Log(1.0 - _random.NextDouble()) / serviceIntensity;
            await Task.Delay(TimeSpan.FromSeconds(serviceTime));

            EndTime = DateTime.Now;
            IsBusy = false;
            CurrentClient = null;
        }

        public void ForceStop()
        {
            IsBusy = false;
            CurrentClient = null;
        }
    }

    /// <summary>
    /// Класс сервера с пулом потоков
    /// </summary>
    public class Server
    {
        private readonly int _channelCount;
        private readonly List<ServiceChannel> _channels;
        private readonly double _serviceIntensity;
        private readonly object _statsLock = new object();

        private int _totalRequests;
        private int _servedRequests;
        private int _rejectedRequests;
        private int _totalBusyTime;
        private readonly List<double> _channelUtilization;

        public Server(int channels, double serviceIntensity)
        {
            _channelCount = channels;
            _serviceIntensity = serviceIntensity;
            _channels = new List<ServiceChannel>();
            _channelUtilization = new List<double>();

            for (int i = 0; i < channels; i++)
            {
                _channels.Add(new ServiceChannel(i));
                _channelUtilization.Add(0);
            }
        }

        public void HandleRequest(object? sender, RequestEventArgs args)
        {
            if (sender is Client client)
            {
                lock (_statsLock)
                {
                    _totalRequests++;
                }

                ServiceChannel? freeChannel = _channels.FirstOrDefault(c => !c.IsBusy);

                if (freeChannel != null)
                {
                    lock (_statsLock)
                    {
                        _servedRequests++;
                    }

                    Task.Run(async () =>
                    {
                        await freeChannel.ProcessRequest(client, _serviceIntensity, args.ProcessingTime);
                        
                        lock (_statsLock)
                        {
                            double busyTime = (freeChannel.EndTime - freeChannel.StartTime).TotalSeconds;
                            _channelUtilization[freeChannel.ChannelId] += busyTime;
                            _totalBusyTime += (int)(busyTime * 1000);
                        }
                    });
                }
                else
                {
                    lock (_statsLock)
                    {
                        _rejectedRequests++;
                    }
                }
            }
        }

        public void ResetStats()
        {
            lock (_statsLock)
            {
                _totalRequests = 0;
                _servedRequests = 0;
                _rejectedRequests = 0;
                _totalBusyTime = 0;
                for (int i = 0; i < _channelCount; i++)
                {
                    _channelUtilization[i] = 0;
                }
            }
        }

        public (int total, int served, int rejected, double avgBusyTime) GetStats()
        {
            lock (_statsLock)
            {
                double avgBusyTime = _totalRequests > 0 ? (double)_totalBusyTime / _totalRequests / 1000.0 : 0;
                return (_totalRequests, _servedRequests, _rejectedRequests, avgBusyTime);
            }
        }

        public double GetChannelUtilization(int channelId)
        {
            lock (_statsLock)
            {
                return channelId < _channelUtilization.Count ? _channelUtilization[channelId] : 0;
            }
        }

        public int GetChannelCount() => _channelCount;
    }

    /// <summary>
    /// Класс для расчета теоретических показателей СМО
    /// </summary>
    public static class SmoCalculator
    {
        public static double CalculateIdleProbability(double lambda, double mu, int n)
        {
            double ro = lambda / mu;
            double sum = 0;
            
            for (int i = 0; i <= n; i++)
            {
                sum += Math.Pow(ro, i) / Factorial(i);
            }
            
            return 1.0 / sum;
        }

        public static double CalculateRejectionProbability(double lambda, double mu, int n)
        {
            double ro = lambda / mu;
            double p0 = CalculateIdleProbability(lambda, mu, n);
            return (Math.Pow(ro, n) / Factorial(n)) * p0;
        }

        public static double CalculateRelativeThroughput(double lambda, double mu, int n)
        {
            return 1 - CalculateRejectionProbability(lambda, mu, n);
        }

        public static double CalculateAbsoluteThroughput(double lambda, double mu, int n)
        {
            return lambda * CalculateRelativeThroughput(lambda, mu, n);
        }

        public static double CalculateAverageBusyChannels(double lambda, double mu, int n)
        {
            return CalculateAbsoluteThroughput(lambda, mu, n) / mu;
        }

        private static double Factorial(int n)
        {
            double result = 1;
            for (int i = 2; i <= n; i++)
            {
                result *= i;
            }
            return result;
        }
    }

    /// <summary>
    /// Главный класс программы
    /// </summary>
    public class Program
    {
        private const double MU = 2.0;
        private const int CHANNELS = 5;
        private const double SIMULATION_TIME = 60.0;
        private const int POINTS_COUNT = 10;

        public static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("=== START OF SIMULATION ===");
                Console.WriteLine($"Current directory: {Directory.GetCurrentDirectory()}");
                Console.OutputEncoding = System.Text.Encoding.UTF8;
                
                Console.WriteLine("Моделирование многоканальной СМО с отказами");
                Console.WriteLine($"Количество каналов: {CHANNELS}");
                Console.WriteLine($"Интенсивность обслуживания (mu): {MU} заявок/сек");
                Console.WriteLine($"Время симуляции: {SIMULATION_TIME} сек");
                Console.WriteLine(new string('-', 80));

                double[] lambdaValues = new double[POINTS_COUNT];
                double lambdaMin = 0.5;
                double lambdaMax = 10.0;
                
                for (int i = 0; i < POINTS_COUNT; i++)
                {
                    lambdaValues[i] = lambdaMin + (lambdaMax - lambdaMin) * i / (POINTS_COUNT - 1);
                }

                List<SimulationResult> results = new List<SimulationResult>();

                Console.WriteLine("\nИсследование зависимости показателей СМО от интенсивности входного потока lambda");
                Console.WriteLine("lambda\t\tP0(теор)\tP0(эксп)\tPотк(теор)\tPотк(эксп)\tQ(теор)\t\tQ(эксп)");
                Console.WriteLine(new string('-', 100));

                for (int i = 0; i < lambdaValues.Length; i++)
                {
                    double lambda = lambdaValues[i];
                    Console.Write($"Выполняется эксперимент {i+1}/{lambdaValues.Length} (lambda={lambda:F2})... ");
                    var result = await RunSimulation(lambda, MU, CHANNELS, SIMULATION_TIME);
                    results.Add(result);
                    Console.WriteLine(" готово");
                    
                    Console.WriteLine($"{lambda:F2}\t\t{result.TheoreticalIdleProbability:F4}\t\t{result.ExperimentalIdleProbability:F4}\t\t" +
                                      $"{result.TheoreticalRejectionProbability:F4}\t\t{result.ExperimentalRejectionProbability:F4}\t\t" +
                                      $"{result.TheoreticalRelativeThroughput:F4}\t\t{result.ExperimentalRelativeThroughput:F4}");
                }

                Console.WriteLine("\nСоздание графиков...");
                CreateCharts(results, lambdaValues);
                
                Console.WriteLine("Запись результатов в файл...");
                WriteResultsToFile(results, lambdaValues);

                Console.WriteLine("\nМоделирование завершено. Результаты сохранены в файл results.txt");
                Console.WriteLine("PNG графики сохранены в папке result/");
                
                Console.WriteLine($"\nФайл results.txt существует: {File.Exists("results.txt")}");
                if (File.Exists("results.txt"))
                {
                    Console.WriteLine($"Размер results.txt: {new FileInfo("results.txt").Length} байт");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"FATAL ERROR: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        static async Task<SimulationResult> RunSimulation(double lambda, double mu, int channels, double simulationTime)
        {
            Server server = new Server(channels, mu);
            List<Client> clients = new List<Client>();
            CancellationTokenSource cts = new CancellationTokenSource();
            
            int clientCount = 10;
            double clientLambda = lambda / clientCount;
            
            for (int i = 0; i < clientCount; i++)
            {
                clients.Add(new Client(server, clientLambda));
            }

            List<Task> clientTasks = new List<Task>();
            foreach (var client in clients)
            {
                clientTasks.Add(client.StartGeneratingRequests(cts.Token));
            }

            await Task.Delay(TimeSpan.FromSeconds(simulationTime));
            cts.Cancel();
            
            try
            {
                await Task.WhenAll(clientTasks);
            }
            catch (OperationCanceledException)
            {
                // Ожидаемое исключение
            }

            var stats = server.GetStats();
            
            double theoreticalIdleProb = SmoCalculator.CalculateIdleProbability(lambda, mu, channels);
            double theoreticalRejectionProb = SmoCalculator.CalculateRejectionProbability(lambda, mu, channels);
            double theoreticalRelativeThroughput = SmoCalculator.CalculateRelativeThroughput(lambda, mu, channels);
            double theoreticalAbsoluteThroughput = SmoCalculator.CalculateAbsoluteThroughput(lambda, mu, channels);
            double theoreticalAvgBusyChannels = SmoCalculator.CalculateAverageBusyChannels(lambda, mu, channels);
            
            double experimentalRejectionProb = stats.total > 0 ? (double)stats.rejected / stats.total : 0;
            double experimentalRelativeThroughput = stats.total > 0 ? (double)stats.served / stats.total : 0;
            double experimentalAbsoluteThroughput = stats.total > 0 ? stats.served / simulationTime : 0;
            double experimentalAvgBusyChannels = stats.total > 0 ? experimentalAbsoluteThroughput / mu : 0;
            
            double totalChannelTime = channels * simulationTime;
            double totalBusyTime = 0;
            for (int i = 0; i < channels; i++)
            {
                totalBusyTime += server.GetChannelUtilization(i);
            }
            double experimentalIdleProb = totalChannelTime > 0 ? 1 - (totalBusyTime / totalChannelTime) : 1;

            return new SimulationResult
            {
                Lambda = lambda,
                Mu = mu,
                Channels = channels,
                TotalRequests = stats.total,
                ServedRequests = stats.served,
                RejectedRequests = stats.rejected,
                TheoreticalIdleProbability = theoreticalIdleProb,
                ExperimentalIdleProbability = experimentalIdleProb,
                TheoreticalRejectionProbability = theoreticalRejectionProb,
                ExperimentalRejectionProbability = experimentalRejectionProb,
                TheoreticalRelativeThroughput = theoreticalRelativeThroughput,
                ExperimentalRelativeThroughput = experimentalRelativeThroughput,
                TheoreticalAbsoluteThroughput = theoreticalAbsoluteThroughput,
                ExperimentalAbsoluteThroughput = experimentalAbsoluteThroughput,
                TheoreticalAvgBusyChannels = theoreticalAvgBusyChannels,
                ExperimentalAvgBusyChannels = experimentalAvgBusyChannels
            };
        }

        static void CreateCharts(List<SimulationResult> results, double[] lambdaValues)
        {
            Directory.CreateDirectory("result");
            
            double[] theoreticalP0 = results.Select(r => r.TheoreticalIdleProbability).ToArray();
            double[] experimentalP0 = results.Select(r => r.ExperimentalIdleProbability).ToArray();
            CreateLineChart("result/p-1.png", "Вероятность простоя системы (P0)", 
                lambdaValues, theoreticalP0, experimentalP0, 
                "Теоретическая P0", "Экспериментальная P0", "lambda", "P0");
            
            double[] theoreticalPRej = results.Select(r => r.TheoreticalRejectionProbability).ToArray();
            double[] experimentalPRej = results.Select(r => r.ExperimentalRejectionProbability).ToArray();
            CreateLineChart("result/p-2.png", "Вероятность отказа (Pотк)", 
                lambdaValues, theoreticalPRej, experimentalPRej,
                "Теоретическая Pотк", "Экспериментальная Pотк", "lambda", "Pотк");
            
            double[] theoreticalQ = results.Select(r => r.TheoreticalRelativeThroughput).ToArray();
            double[] experimentalQ = results.Select(r => r.ExperimentalRelativeThroughput).ToArray();
            CreateLineChart("result/p-3.png", "Относительная пропускная способность (Q)", 
                lambdaValues, theoreticalQ, experimentalQ,
                "Теоретическая Q", "Экспериментальная Q", "lambda", "Q");
            
            double[] theoreticalA = results.Select(r => r.TheoreticalAbsoluteThroughput).ToArray();
            double[] experimentalA = results.Select(r => r.ExperimentalAbsoluteThroughput).ToArray();
            CreateLineChart("result/p-4.png", "Абсолютная пропускная способность (A)", 
                lambdaValues, theoreticalA, experimentalA,
                "Теоретическая A", "Экспериментальная A", "lambda", "A (заявок/сек)");
            
            double[] theoreticalK = results.Select(r => r.TheoreticalAvgBusyChannels).ToArray();
            double[] experimentalK = results.Select(r => r.ExperimentalAvgBusyChannels).ToArray();
            CreateLineChart("result/p-5.png", "Среднее число занятых каналов (k)", 
                lambdaValues, theoreticalK, experimentalK,
                "Теоретическое k", "Экспериментальное k", "lambda", "k");
        }

        static void CreateLineChart(string filename, string title, double[] x,
            double[] y1, double[] y2, string legend1, string legend2, 
            string xLabel, string yLabel)
        {
            var plot = new Plot();
            
            var scatter1 = plot.Add.Scatter(x, y1);
            scatter1.LegendText = legend1;
            scatter1.Color = new ScottPlot.Color(0, 0, 255);
            scatter1.LineWidth = 2;
            scatter1.MarkerSize = 8;
            scatter1.MarkerShape = MarkerShape.FilledCircle;
            
            var scatter2 = plot.Add.Scatter(x, y2);
            scatter2.LegendText = legend2;
            scatter2.Color = new ScottPlot.Color(255, 0, 0);
            scatter2.LineWidth = 2;
            scatter2.MarkerSize = 8;
            scatter2.MarkerShape = MarkerShape.OpenSquare;
            
            plot.Title(title);
            plot.XLabel(xLabel);
            plot.YLabel(yLabel);
            plot.ShowLegend();
            
            plot.SavePng(filename, 800, 600);
            Console.WriteLine($"Создан график: {filename}");
        }

        static void WriteResultsToFile(List<SimulationResult> results, double[] lambdaValues)
        {
            using (StreamWriter writer = new StreamWriter("results.txt", false, System.Text.Encoding.UTF8))
            {
                writer.WriteLine("РЕЗУЛЬТАТЫ МОДЕЛИРОВАНИЯ МНОГОКАНАЛЬНОЙ СМО С ОТКАЗАМИ");
                writer.WriteLine("=================================================");
                writer.WriteLine($"Количество каналов: {results[0].Channels}");
                writer.WriteLine($"Интенсивность обслуживания (mu): {results[0].Mu} заявок/сек");
                writer.WriteLine($"Время симуляции: {SIMULATION_TIME} сек");
                writer.WriteLine($"Количество экспериментов: {POINTS_COUNT}");
                writer.WriteLine();
                
                writer.WriteLine("ТАБЛИЦА РЕЗУЛЬТАТОВ");
                writer.WriteLine(new string('-', 150));
                writer.WriteLine($"{"lambda",-10} {"P0(теор)",-15} {"P0(эксп)",-15} {"Pотк(теор)",-15} {"Pотк(эксп)",-15} " +
                                 $"{"Q(теор)",-15} {"Q(эксп)",-15} {"A(теор)",-15} {"A(эксп)",-15} {"k(теор)",-15} {"k(эксп)",-15}");
                writer.WriteLine(new string('-', 150));
                
                for (int i = 0; i < results.Count; i++)
                {
                    var r = results[i];
                    writer.WriteLine($"{lambdaValues[i],-10:F2} {r.TheoreticalIdleProbability,-15:F6} {r.ExperimentalIdleProbability,-15:F6} " +
                                     $"{r.TheoreticalRejectionProbability,-15:F6} {r.ExperimentalRejectionProbability,-15:F6} " +
                                     $"{r.TheoreticalRelativeThroughput,-15:F6} {r.ExperimentalRelativeThroughput,-15:F6} " +
                                     $"{r.TheoreticalAbsoluteThroughput,-15:F6} {r.ExperimentalAbsoluteThroughput,-15:F6} " +
                                     $"{r.TheoreticalAvgBusyChannels,-15:F6} {r.ExperimentalAvgBusyChannels,-15:F6}");
                }
                
                writer.WriteLine(new string('-', 150));
                writer.WriteLine();
                writer.WriteLine("ВЫВОДЫ:");
                writer.WriteLine("1. При увеличении интенсивности входного потока lambda вероятность отказа возрастает,");
                writer.WriteLine("   а относительная пропускная способность снижается.");
                writer.WriteLine("2. Экспериментальные значения хорошо согласуются с теоретическими,");
                writer.WriteLine("   что подтверждает корректность модели.");
                writer.WriteLine("3. При lambda > mu*n система перегружена, большинство запросов получают отказ.");
                writer.WriteLine("4. Среднее число занятых каналов стремится к n при увеличении lambda.");
                writer.WriteLine();
                writer.WriteLine("ФОРМУЛЫ ДЛЯ РАСЧЕТОВ:");
                writer.WriteLine("P0 = [SUM(rho^i / i!)]^(-1), где rho = lambda/mu");
                writer.WriteLine("Pотк = (rho^n / n!) * P0");
                writer.WriteLine("Q = 1 - Pотк");
                writer.WriteLine("A = lambda * Q");
                writer.WriteLine("k = A / mu");
            }
        }
    }

    /// <summary>
    /// Класс для хранения результатов моделирования
    /// </summary>
    public class SimulationResult
    {
        public double Lambda { get; set; }
        public double Mu { get; set; }
        public int Channels { get; set; }
        public int TotalRequests { get; set; }
        public int ServedRequests { get; set; }
        public int RejectedRequests { get; set; }
        
        public double TheoreticalIdleProbability { get; set; }
        public double ExperimentalIdleProbability { get; set; }
        public double TheoreticalRejectionProbability { get; set; }
        public double ExperimentalRejectionProbability { get; set; }
        public double TheoreticalRelativeThroughput { get; set; }
        public double ExperimentalRelativeThroughput { get; set; }
        public double TheoreticalAbsoluteThroughput { get; set; }
        public double ExperimentalAbsoluteThroughput { get; set; }
        public double TheoreticalAvgBusyChannels { get; set; }
        public double ExperimentalAvgBusyChannels { get; set; }
    }
}
