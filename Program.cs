using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SmoModeling
{
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
            Id = _nextId++;
            _server = server;
            _requestRate = requestRate;
            _random = new Random();
        }
        
        public async Task StartGeneratingRequests(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                double interval = -Math.Log(1.0 - _random.NextDouble()) / _requestRate;
                await Task.Delay(TimeSpan.FromSeconds(interval), cancellationToken);
                
                var request = new Request(Id, DateTime.Now);
                OnRequestGenerated(request);
            }
        }
        
        protected virtual void OnRequestGenerated(Request request)
        {
            RequestGenerated?.Invoke(this, new RequestEventArgs(request));
        }
    }
    
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
    
    public class RequestEventArgs : EventArgs
    {
        public Request Request { get; }
        public RequestEventArgs(Request request) => Request = request;
    }
    
    public class ServiceChannel
    {
        public int Id { get; }
        public bool IsBusy { get; private set; }
        private readonly double _serviceRate;
        
        public ServiceChannel(int id, double serviceRate)
        {
            Id = id;
            _serviceRate = serviceRate;
            IsBusy = false;
        }
        
        public async Task ProcessRequest(Request request)
        {
            IsBusy = true;
            request.StartTime = DateTime.Now;
            
            var random = new Random();
            double serviceTime = -Math.Log(1.0 - random.NextDouble()) / _serviceRate;
            await Task.Delay(TimeSpan.FromSeconds(serviceTime));
            
            request.EndTime = DateTime.Now;
            IsBusy = false;
        }
    }
    
    public class Server
    {
        private readonly List<ServiceChannel> _channels;
        private readonly object _statsLock = new object();
        
        private int _totalRequests = 0;
        private int _processedRequests = 0;
        private int _rejectedRequests = 0;
        private double _totalBusyTime = 0;
        private readonly Stopwatch _systemUptime = new Stopwatch();
        
        public Server(int channelCount, double serviceRate)
        {
            _channels = new List<ServiceChannel>();
            for (int i = 0; i < channelCount; i++)
            {
                _channels.Add(new ServiceChannel(i + 1, serviceRate));
            }
            _systemUptime.Start();
        }
        
        public void SubscribeClient(Client client)
        {
            client.RequestGenerated += OnRequestReceived;
        }
        
        private async void OnRequestReceived(object sender, RequestEventArgs e)
        {
            lock (_statsLock) _totalRequests++;
            
            var freeChannel = _channels.FirstOrDefault(c => !c.IsBusy);
            
            if (freeChannel != null)
            {
                lock (_statsLock) _processedRequests++;
                await freeChannel.ProcessRequest(e.Request);
            }
            else
            {
                lock (_statsLock) _rejectedRequests++;
                e.Request.StartTime = DateTime.Now;
                e.Request.EndTime = DateTime.Now;
            }
            
            lock (_statsLock)
            {
                _totalBusyTime += _channels.Count(c => c.IsBusy);
            }
        }
        
        public Statistics GetStatistics()
        {
            lock (_statsLock)
            {
                double uptime = _systemUptime.Elapsed.TotalSeconds;
                double avgBusyChannels = uptime > 0 ? _totalBusyTime / uptime : 0;
                
                return new Statistics
                {
                    TotalRequests = _totalRequests,
                    ProcessedRequests = _processedRequests,
                    RejectedRequests = _rejectedRequests,
                    AvgBusyChannels = avgBusyChannels,
                    Uptime = uptime
                };
            }
        }
    }
    
    public class Statistics
    {
        public int TotalRequests { get; set; }
        public int ProcessedRequests { get; set; }
        public int RejectedRequests { get; set; }
        public double AvgBusyChannels { get; set; }
        public double Uptime { get; set; }
        
        public double ProbabilityRejection => TotalRequests > 0 ? (double)RejectedRequests / TotalRequests : 0;
        public double RelativeThroughput => TotalRequests > 0 ? (double)ProcessedRequests / TotalRequests : 0;
        public double AbsoluteThroughput => ProcessedRequests / Uptime;
        public double ProbabilityIdle => 1 - (AvgBusyChannels / 5);
    }
    
    public static class SmoTheory
    {
        public static TheoreticalResults Calculate(double lambda, double mu, int n)
        {
            double rho = lambda / mu;
            double sum = 0;
            
            for (int k = 0; k <= n; k++)
                sum += Math.Pow(rho, k) / Factorial(k);
            
            double p0 = 1 / sum;
            double pRejection = (Math.Pow(rho, n) / Factorial(n)) * p0;
            double relativeThroughput = 1 - pRejection;
            double absoluteThroughput = lambda * relativeThroughput;
            double avgBusyChannels = rho * (1 - pRejection);
            
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
    
    class Program
    {
        private const int ChannelCount = 5;
        private const double Mu = 2.0;
        
        static async Task Main(string[] args)
        {
            Console.WriteLine("МОДЕЛИРОВАНИЕ МНОГОКАНАЛЬНОЙ СМО С ОТКАЗАМИ");
            Console.WriteLine($"Каналов: {ChannelCount}, μ = {Mu} запросов/сек\n");
            
            var lambdaValues = new List<double>();
            for (double lambda = 0.5; lambda <= 5.5 + 0.1; lambda += 0.5)
                lambdaValues.Add(Math.Round(lambda, 1));
            
            var experimentalResults = new List<Statistics>();
            var theoreticalResults = new List<TheoreticalResults>();
            
            foreach (var lambda in lambdaValues)
            {
                Console.WriteLine($"\n--- λ = {lambda:F2} ---");
                
                var server = new Server(ChannelCount, Mu);
                var clients = new List<Client>();
                var cts = new CancellationTokenSource();
                
                int clientCount = 5;
                double clientRate = lambda / clientCount;
                
                for (int i = 0; i < clientCount; i++)
                {
                    var client = new Client(server, clientRate);
                    server.SubscribeClient(client);
                    clients.Add(client);
                }
                
                var tasks = clients.Select(c => c.StartGeneratingRequests(cts.Token)).ToArray();
                
                Console.WriteLine("Моделирование 60 сек...");
                await Task.Delay(60000);
                cts.Cancel();
                
                try { await Task.WhenAll(tasks); }
                catch (OperationCanceledException) { }
                
                var stats = server.GetStatistics();
                var theoretical = SmoTheory.Calculate(lambda, Mu, ChannelCount);
                
                experimentalResults.Add(stats);
                theoreticalResults.Add(theoretical);
                
                Console.WriteLine($"Поступило: {stats.TotalRequests}, Обслужено: {stats.ProcessedRequests}, Отказ: {stats.RejectedRequests}");
                Console.WriteLine($"Pотк эксп: {stats.ProbabilityRejection:F4}, теор: {theoretical.ProbabilityRejection:F4}");
            }
            
            Directory.CreateDirectory("result");
            SaveResultsToFile(experimentalResults, theoreticalResults, lambdaValues);
            GenerateTextGraphs(experimentalResults, theoreticalResults, lambdaValues);
            GenerateSimpleCsvFiles(experimentalResults, theoreticalResults, lambdaValues);
            GenerateHtmlReport(experimentalResults, theoreticalResults, lambdaValues);
            
            Console.WriteLine("\n\nРЕЗУЛЬТАТЫ СОХРАНЕНЫ В ПАПКУ 'result':");
            Console.WriteLine("  - results.txt - таблица результатов");
            Console.WriteLine("  - report.html - отчет с графиками (открыть в браузере)");
            Console.WriteLine("  - data_*.csv - данные для построения графиков");
            Console.WriteLine("\nОТКРОЙТЕ ФАЙЛ result/report.html В ЛЮБОМ БРАУЗЕРЕ");
        }
        
        static void SaveResultsToFile(List<Statistics> experimental, List<TheoreticalResults> theoretical, List<double> lambdaValues)
        {
            using (var writer = new StreamWriter("result/results.txt", false, System.Text.Encoding.UTF8))
            {
                writer.WriteLine("РЕЗУЛЬТАТЫ МОДЕЛИРОВАНИЯ МНОГОКАНАЛЬНОЙ СМО С ОТКАЗАМИ");
                writer.WriteLine("=====================================================");
                writer.WriteLine($"Каналов: 5 | μ = 2.0 запросов/сек\n");
                writer.WriteLine("λ\tP0(эксп)\tP0(теор)\tPотк(эксп)\tPотк(теор)\tQ(эксп)\tQ(теор)\tA(эксп)\tA(теор)\tk(эксп)\tk(теор)");
                
                for (int i = 0; i < lambdaValues.Count; i++)
                {
                    writer.WriteLine($"{lambdaValues[i]:F2}\t{experimental[i].ProbabilityIdle:F4}\t{theoretical[i].ProbabilityIdle:F4}\t{experimental[i].ProbabilityRejection:F4}\t{theoretical[i].ProbabilityRejection:F4}\t{experimental[i].RelativeThroughput:F4}\t{theoretical[i].RelativeThroughput:F4}\t{experimental[i].AbsoluteThroughput:F2}\t{theoretical[i].AbsoluteThroughput:F2}\t{experimental[i].AvgBusyChannels:F4}\t{theoretical[i].AvgBusyChannels:F4}");
                }
            }
        }
        
        static void GenerateTextGraphs(List<Statistics> experimental, List<TheoreticalResults> theoretical, List<double> lambdaValues)
        {
            using (var writer = new StreamWriter("result/text_graphs.txt", false, System.Text.Encoding.UTF8))
            {
                writer.WriteLine("ГРАФИК 1: Вероятность отказа Pотк\n");
                writer.WriteLine(" λ     | Теоретическое | Экспериментальное | Визуализация");
                writer.WriteLine("-------+---------------+-------------------+----------------");
                
                for (int i = 0; i < lambdaValues.Count; i++)
                {
                    int theoBars = (int)(theoretical[i].ProbabilityRejection * 50);
                    int expBars = (int)(experimental[i].ProbabilityRejection * 50);
                    writer.WriteLine($"{lambdaValues[i]:F2}    | {theoretical[i].ProbabilityRejection:F4}     | {experimental[i].ProbabilityRejection:F4}        | Теор: {new string('█', theoBars)}");
                    writer.WriteLine($"       |               |                   | Эксп: {new string('█', expBars)}");
                }
                
                writer.WriteLine("\n\nГРАФИК 2: Среднее число занятых каналов k\n");
                writer.WriteLine(" λ     | Теоретическое | Экспериментальное | Визуализация");
                writer.WriteLine("-------+---------------+-------------------+----------------");
                
                for (int i = 0; i < lambdaValues.Count; i++)
                {
                    int theoBars = (int)(theoretical[i].AvgBusyChannels / 5 * 50);
                    int expBars = (int)(experimental[i].AvgBusyChannels / 5 * 50);
                    writer.WriteLine($"{lambdaValues[i]:F2}    | {theoretical[i].AvgBusyChannels:F4}     | {experimental[i].AvgBusyChannels:F4}        | Теор: {new string('█', theoBars)}");
                    writer.WriteLine($"       |               |                   | Эксп: {new string('█', expBars)}");
                }
            }
        }
        
        static void GenerateSimpleCsvFiles(List<Statistics> experimental, List<TheoreticalResults> theoretical, List<double> lambdaValues)
        {
            // CSV для каждого графика
            WriteCsv("result/p1_idle.csv", lambdaValues, 
                experimental.Select(x => x.ProbabilityIdle).ToList(),
                theoretical.Select(x => x.ProbabilityIdle).ToList(),
                "P0_эксп", "P0_теор");
            
            WriteCsv("result/p2_rejection.csv", lambdaValues,
                experimental.Select(x => x.ProbabilityRejection).ToList(),
                theoretical.Select(x => x.ProbabilityRejection).ToList(),
                "Pотк_эксп", "Pотк_теор");
            
            WriteCsv("result/p3_throughput.csv", lambdaValues,
                experimental.Select(x => x.RelativeThroughput).ToList(),
                theoretical.Select(x => x.RelativeThroughput).ToList(),
                "Q_эксп", "Q_теор");
            
            WriteCsv("result/p4_absolute.csv", lambdaValues,
                experimental.Select(x => x.AbsoluteThroughput).ToList(),
                theoretical.Select(x => x.AbsoluteThroughput).ToList(),
                "A_эксп", "A_теор");
            
            WriteCsv("result/p5_channels.csv", lambdaValues,
                experimental.Select(x => x.AvgBusyChannels).ToList(),
                theoretical.Select(x => x.AvgBusyChannels).ToList(),
                "k_эксп", "k_теор");
        }
        
        static void WriteCsv(string filename, List<double> lambda, List<double> exp, List<double> theo, string expName, string theoName)
        {
            using (var writer = new StreamWriter(filename, false, System.Text.Encoding.UTF8))
            {
                writer.WriteLine($"Lambda,{expName},{theoName}");
                for (int i = 0; i < lambda.Count; i++)
                    writer.WriteLine($"{lambda[i]:F2},{exp[i]:F4},{theo[i]:F4}");
            }
        }
        
        static void GenerateHtmlReport(List<Statistics> experimental, List<TheoreticalResults> theoretical, List<double> lambdaValues)
        {
            string html = $@"<!DOCTYPE html>
<html>
<head>
    <meta charset='UTF-8'>
    <title>СМО Отчет</title>
    <style>
        body {{ font-family: Arial; margin: 20px; background: #f0f0f0; }}
        .container {{ max-width: 1200px; margin: auto; background: white; padding: 20px; border-radius: 10px; }}
        h1, h2 {{ text-align: center; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: center; }}
        th {{ background: #4CAF50; color: white; }}
        .chart {{ margin: 30px 0; padding: 20px; background: #f9f9f9; border-radius: 5px; }}
        .bar {{ background: #4CAF50; height: 30px; margin: 5px 0; color: white; padding: 5px; }}
        .legend {{ display: inline-block; width: 20px; height: 20px; margin: 0 10px; }}
        .green {{ background: #4CAF50; }}
        .red {{ background: #f44336; }}
    </style>
</head>
<body>
<div class='container'>
    <h1>Многоканальная СМО с отказами</h1>
    <h3>Параметры: n = 5 каналов, μ = 2.0 запросов/сек</h3>
    
    <h2>Таблица результатов</h2>
    <table>
        <tr>
            <th>λ</th><th>P₀ эксп</th><th>P₀ теор</th>
            <th>P<sub>отк</sub> эксп</th><th>P<sub>отк</sub> теор</th>
            <th>Q эксп</th><th>Q теор</th>
            <th>A эксп</th><th>A теор</th>
            <th>k эксп</th><th>k теор</th>
        </tr>";
            
            for (int i = 0; i < lambdaValues.Count; i++)
            {
                html += $@"
        <tr>
            <td>{lambdaValues[i]:F2}</td>
            <td>{experimental[i].ProbabilityIdle:F4}</td><td>{theoretical[i].ProbabilityIdle:F4}</td>
            <td>{experimental[i].ProbabilityRejection:F4}</td><td>{theoretical[i].ProbabilityRejection:F4}</td>
            <td>{experimental[i].RelativeThroughput:F4}</td><td>{theoretical[i].RelativeThroughput:F4}</td>
            <td>{experimental[i].AbsoluteThroughput:F2}</td><td>{theoretical[i].AbsoluteThroughput:F2}</td>
            <td>{experimental[i].AvgBusyChannels:F4}</td><td>{theoretical[i].AvgBusyChannels:F4}</td>
        </tr>";
            }
            
        html += @"</table>
    
    <h2>График 1: Вероятность отказа Pотк</h2>
    <div class='chart'>";
            
        for (int i = 0; i < lambdaValues.Count; i++)
        {
            int expWidth = (int)(experimental[i].ProbabilityRejection * 300);
            int theoWidth = (int)(theoretical[i].ProbabilityRejection * 300);
            html += $@"
        <div><b>λ={lambdaValues[i]:F2}</b></div>
        <div style='background:#f44336; width:{theoWidth}px; margin:2px 0; padding:2px; color:white;'>Теор: {experimental[i].ProbabilityRejection:F3}</div>
        <div style='background:#4CAF50; width:{expWidth}px; margin:2px 0; padding:2px; color:white;'>Эксп: {theoretical[i].ProbabilityRejection:F3}</div>";
        }
        
        html += @"</div>
    
    <h2>График 2: Среднее число занятых каналов k</h2>
    <div class='chart'>";
        
        for (int i = 0; i < lambdaValues.Count; i++)
        {
            int expWidth = (int)(experimental[i].AvgBusyChannels / 5 * 300);
            int theoWidth = (int)(theoretical[i].AvgBusyChannels / 5 * 300);
            html += $@"
        <div><b>λ={lambdaValues[i]:F2}</b></div>
        <div style='background:#f44336; width:{theoWidth}px; margin:2px 0; padding:2px; color:white;'>Теор: {experimental[i].AvgBusyChannels:F2}</div>
        <div style='background:#4CAF50; width:{expWidth}px; margin:2px 0; padding:2px; color:white;'>Эксп: {theoretical[i].AvgBusyChannels:F2}</div>";
        }
        
        html += @"</div>
    
    <h2>CSV файлы для построения графиков</h2>
    <ul>
        <li><a href='p1_idle.csv'>p1_idle.csv</a> - Вероятность простоя</li>
        <li><a href='p2_rejection.csv'>p2_rejection.csv</a> - Вероятность отказа</li>
        <li><a href='p3_throughput.csv'>p3_throughput.csv</a> - Относительная пропускная способность</li>
        <li><a href='p4_absolute.csv'>p4_absolute.csv</a> - Абсолютная пропускная способность</li>
        <li><a href='p5_channels.csv'>p5_channels.csv</a> - Среднее число занятых каналов</li>
    </ul>
    <p><i>Откройте CSV файлы в Excel и постройте графики: Вставка → Точечная диаграмма</i></p>
</div>
</body>
</html>";
            
            File.WriteAllText("result/report.html", html, System.Text.Encoding.UTF8);
        }
    }
}
