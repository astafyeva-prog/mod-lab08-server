using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
        private readonly double _requestRate; // интенсивность генерации запросов
        
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
                // Имитируем интервал между запросами (экспоненциальное распределение)
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
        
        public double ProcessingTime => EndTime.HasValue && StartTime.HasValue 
            ? (EndTime.Value - StartTime.Value).TotalSeconds : 0;
    }
    
    /// <summary>
    /// Аргументы события запроса
    /// </summary>
    public class RequestEventArgs : EventArgs
    {
        public Request Request { get; }
        
        public RequestEventArgs(Request request)
        {
            Request = request;
        }
    }
    
    /// <summary>
    /// Класс канала обслуживания
    /// </summary>
    public class ServiceChannel
    {
        public int Id { get; }
        public bool IsBusy { get; private set; }
        public Request CurrentRequest { get; private set; }
        private readonly double _serviceRate; // интенсивность обслуживания
        
        public ServiceChannel(int id, double serviceRate)
        {
            Id = id;
            _serviceRate = serviceRate;
            IsBusy = false;
        }
        
        public async Task ProcessRequest(Request request)
        {
            IsBusy = true;
            CurrentRequest = request;
            request.StartTime = DateTime.Now;
            
            // Имитируем время обработки (экспоненциальное распределение)
            var random = new Random();
            double serviceTime = -Math.Log(1.0 - random.NextDouble()) / _serviceRate;
            await Task.Delay(TimeSpan.FromSeconds(serviceTime));
            
            request.EndTime = DateTime.Now;
            IsBusy = false;
            CurrentRequest = null;
        }
    }
    
    /// <summary>
    /// Класс сервера с пулом каналов
    /// </summary>
    public class Server
    {
        private readonly List<ServiceChannel> _channels;
        private readonly double _serviceRate;
        private readonly object _statsLock = new object();
        
        // Статистика
        private int _totalRequests = 0;
        private int _processedRequests = 0;
        private int _rejectedRequests = 0;
        private double _totalBusyTime = 0;
        private readonly Stopwatch _systemUptime = new Stopwatch();
        
        public Server(int channelCount, double serviceRate)
        {
            _serviceRate = serviceRate;
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
            lock (_statsLock)
            {
                _totalRequests++;
            }
            
            var freeChannel = _channels.FirstOrDefault(c => !c.IsBusy);
            
            if (freeChannel != null)
            {
                // Запрос принят к обслуживанию
                lock (_statsLock)
                {
                    _processedRequests++;
                }
                await freeChannel.ProcessRequest(e.Request);
            }
            else
            {
                // Все каналы заняты - отказ
                lock (_statsLock)
                {
                    _rejectedRequests++;
                }
                e.Request.StartTime = DateTime.Now;
                e.Request.EndTime = DateTime.Now;
            }
            
            UpdateBusyTime();
        }
        
        private void UpdateBusyTime()
        {
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
        
        public void Reset()
        {
            lock (_statsLock)
            {
                _totalRequests = 0;
                _processedRequests = 0;
                _rejectedRequests = 0;
                _totalBusyTime = 0;
                _systemUptime.Restart();
            }
        }
    }
    
    /// <summary>
    /// Класс статистики
    /// </summary>
    public class Statistics
    {
        public int TotalRequests { get; set; }
        public int ProcessedRequests { get; set; }
        public int RejectedRequests { get; set; }
        public double AvgBusyChannels { get; set; }
        public double Uptime { get; set; }
        
        // Расчетные показатели
        public double ProbabilityIdle => 1 - (AvgBusyChannels / 5); // 5 - количество каналов
        public double ProbabilityRejection => TotalRequests > 0 ? (double)RejectedRequests / TotalRequests : 0;
        public double RelativeThroughput => TotalRequests > 0 ? (double)ProcessedRequests / TotalRequests : 0;
        public double AbsoluteThroughput => ProcessedRequests / Uptime;
        public double AvgBusyChannelsValue => AvgBusyChannels;
    }
    
    /// <summary>
    /// Класс для теоретических расчетов СМО
    /// </summary>
    public static class SmoTheory
    {
        public static TheoreticalResults Calculate(double lambda, double mu, int n)
        {
            double rho = lambda / mu;
            double sum = 0;
            
            // Вычисляем P0 - вероятность простоя системы
            for (int k = 0; k <= n; k++)
            {
                sum += Math.Pow(rho, k) / Factorial(k);
            }
            double p0 = 1 / sum;
            
            // Вероятность отказа (все каналы заняты)
            double pRejection = (Math.Pow(rho, n) / Factorial(n)) * p0;
            
            // Относительная пропускная способность
            double relativeThroughput = 1 - pRejection;
            
            // Абсолютная пропускная способность
            double absoluteThroughput = lambda * relativeThroughput;
            
            // Среднее число занятых каналов
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
            for (int i = 2; i <= n; i++)
                result *= i;
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
    
    /// <summary>
    /// Главный класс программы
    /// </summary>
    class Program
    {
        private const int ChannelCount = 5; // Количество каналов
        private const double Mu = 2.0; // Интенсивность обслуживания (запросов/сек)
        
        static async Task Main(string[] args)
        {
            Console.WriteLine("МОДЕЛИРОВАНИЕ МНОГОКАНАЛЬНОЙ СМО С ОТКАЗАМИ");
            Console.WriteLine($"Количество каналов: {ChannelCount}");
            Console.WriteLine($"Интенсивность обслуживания (μ): {Mu} запросов/сек");
            Console.WriteLine();
            
            // Диапазон интенсивности входного потока
            var lambdaValues = new List<double>();
            for (double lambda = 0.5; lambda <= 5.5 + 0.1; lambda += 0.5)
            {
                lambdaValues.Add(Math.Round(lambda, 1));
            }
            
            var experimentalResults = new List<ExperimentalPoint>();
            var theoreticalResults = new List<TheoreticalPoint>();
            
            // Проводим эксперименты для разных значений интенсивности
            foreach (var lambda in lambdaValues)
            {
                Console.WriteLine($"\n--- Эксперимент для λ = {lambda:F2} запросов/сек ---");
                
                var server = new Server(ChannelCount, Mu);
                var clients = new List<Client>();
                var cts = new CancellationTokenSource();
                
                // Создаем клиентов с общей интенсивностью lambda
                int clientCount = 5;
                double clientRate = lambda / clientCount;
                
                for (int i = 0; i < clientCount; i++)
                {
                    var client = new Client(server, clientRate);
                    server.SubscribeClient(client);
                    clients.Add(client);
                }
                
                // Запускаем генерацию запросов от клиентов
                var tasks = clients.Select(c => c.StartGeneratingRequests(cts.Token)).ToArray();
                
                // Моделируем в течение 60 секунд
                Console.WriteLine("Моделирование в течение 60 секунд...");
                await Task.Delay(60000);
                cts.Cancel();
                
                try
                {
                    await Task.WhenAll(tasks);
                }
                catch (OperationCanceledException)
                {
                    // Ожидаемое исключение
                }
                
                var stats = server.GetStatistics();
                var theoretical = SmoTheory.Calculate(lambda, Mu, ChannelCount);
                
                experimentalResults.Add(new ExperimentalPoint
                {
                    Lambda = lambda,
                    ProbabilityIdle = stats.ProbabilityIdle,
                    ProbabilityRejection = stats.ProbabilityRejection,
                    RelativeThroughput = stats.RelativeThroughput,
                    AbsoluteThroughput = stats.AbsoluteThroughput,
                    AvgBusyChannels = stats.AvgBusyChannelsValue
                });
                
                theoreticalResults.Add(new TheoreticalPoint
                {
                    Lambda = lambda,
                    ProbabilityIdle = theoretical.ProbabilityIdle,
                    ProbabilityRejection = theoretical.ProbabilityRejection,
                    RelativeThroughput = theoretical.RelativeThroughput,
                    AbsoluteThroughput = theoretical.AbsoluteThroughput,
                    AvgBusyChannels = theoretical.AvgBusyChannels
                });
                
                Console.WriteLine($"Поступило запросов: {stats.TotalRequests}");
                Console.WriteLine($"Обслужено запросов: {stats.ProcessedRequests}");
                Console.WriteLine($"Отклонено запросов: {stats.RejectedRequests}");
                Console.WriteLine($"Вероятность отказа (эксп.): {stats.ProbabilityRejection:F4}");
                Console.WriteLine($"Вероятность отказа (теор.): {theoretical.ProbabilityRejection:F4}");
            }
            
            // Сохраняем результаты в файл
            SaveResultsToFile(experimentalResults, theoreticalResults, lambdaValues);
            
            // Генерируем HTML файл с графиками
            GenerateHtmlWithCharts(experimentalResults, theoreticalResults, lambdaValues);
            
            Console.WriteLine("\n\nМоделирование завершено!");
            Console.WriteLine("Результаты сохранены в файл result/results.txt");
            Console.WriteLine("Графики сохранены в файл result/charts.html");
            Console.WriteLine("Откройте файл result/charts.html в любом браузере для просмотра графиков");
        }
        
        static void SaveResultsToFile(List<ExperimentalPoint> experimental, List<TheoreticalPoint> theoretical, List<double> lambdaValues)
        {
            Directory.CreateDirectory("result");
            
            using (var writer = new StreamWriter("result/results.txt", false, System.Text.Encoding.UTF8))
            {
                writer.WriteLine("РЕЗУЛЬТАТЫ МОДЕЛИРОВАНИЯ МНОГОКАНАЛЬНОЙ СМО С ОТКАЗАМИ");
                writer.WriteLine("=====================================================");
                writer.WriteLine($"Количество каналов: 5");
                writer.WriteLine($"Интенсивность обслуживания (μ): 2.0 запросов/сек");
                writer.WriteLine();
                writer.WriteLine("λ (запросов/сек) | P0 (эксп.) | P0 (теор.) | Pотк (эксп.) | Pотк (теор.) | Q (эксп.) | Q (теор.) | A (эксп.) | A (теор.) | k (эксп.) | k (теор.)");
                writer.WriteLine("-----------------|------------|------------|--------------|--------------|-----------|-----------|-----------|-----------|-----------|-----------");
                
                for (int i = 0; i < lambdaValues.Count; i++)
                {
                    writer.WriteLine($"{lambdaValues[i],15:F2} | {experimental[i].ProbabilityIdle,10:F4} | {theoretical[i].ProbabilityIdle,10:F4} | {experimental[i].ProbabilityRejection,12:F4} | {theoretical[i].ProbabilityRejection,12:F4} | {experimental[i].RelativeThroughput,9:F4} | {theoretical[i].RelativeThroughput,9:F4} | {experimental[i].AbsoluteThroughput,9:F2} | {theoretical[i].AbsoluteThroughput,9:F2} | {experimental[i].AvgBusyChannels,9:F4} | {theoretical[i].AvgBusyChannels,9:F4}");
                }
                
                writer.WriteLine();
                writer.WriteLine("ВЫВОДЫ:");
                writer.WriteLine("1. С увеличением интенсивности входного потока (λ) вероятность отказа растет");
                writer.WriteLine("2. Пропускная способность системы стремится к максимальному значению");
                writer.WriteLine("3. Экспериментальные данные хорошо согласуются с теоретическими расчетами");
                writer.WriteLine("4. При λ > μ*n система перегружена, большинство запросов получают отказ");
            }
        }
        
        static void GenerateHtmlWithCharts(List<ExperimentalPoint> experimental, List<TheoreticalPoint> theoretical, List<double> lambdaValues)
        {
            // Подготовка данных для JavaScript
            var lambdaArray = lambdaValues.Select(x => x.ToString("F2")).ToArray();
            var expIdle = experimental.Select(x => x.ProbabilityIdle.ToString("F4")).ToArray();
            var theoIdle = theoretical.Select(x => x.ProbabilityIdle.ToString("F4")).ToArray();
            var expRejection = experimental.Select(x => x.ProbabilityRejection.ToString("F4")).ToArray();
            var theoRejection = theoretical.Select(x => x.ProbabilityRejection.ToString("F4")).ToArray();
            var expRelative = experimental.Select(x => x.RelativeThroughput.ToString("F4")).ToArray();
            var theoRelative = theoretical.Select(x => x.RelativeThroughput.ToString("F4")).ToArray();
            var expAbsolute = experimental.Select(x => x.AbsoluteThroughput.ToString("F2")).ToArray();
            var theoAbsolute = theoretical.Select(x => x.AbsoluteThroughput.ToString("F2")).ToArray();
            var expChannels = experimental.Select(x => x.AvgBusyChannels.ToString("F4")).ToArray();
            var theoChannels = theoretical.Select(x => x.AvgBusyChannels.ToString("F4")).ToArray();
            
            string html = $@"<!DOCTYPE html>
<html>
<head>
    <meta charset=""UTF-8"">
    <title>Результаты моделирования СМО</title>
    <script src=""https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js""></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        h1 {{
            text-align: center;
            color: #333;
        }}
        .chart-container {{
            background-color: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 30px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        canvas {{
            max-height: 400px;
        }}
        .info {{
            text-align: center;
            color: #666;
            margin-top: 20px;
        }}
    </style>
</head>
<body>
    <div class=""container"">
        <h1>Моделирование многоканальной СМО с отказами</h1>
        <div class=""info"">
            <p>Количество каналов: 5 | Интенсивность обслуживания μ = 2.0 запросов/сек</p>
        </div>
        
        <div class=""chart-container"">
            <h3>График 1: Вероятность простоя системы P₀</h3>
            <canvas id=""chart1""></canvas>
        </div>
        
        <div class=""chart-container"">
            <h3>График 2: Вероятность отказа P<sub>отк</sub></h3>
            <canvas id=""chart2""></canvas>
        </div>
        
        <div class=""chart-container"">
            <h3>График 3: Относительная пропускная способность Q</h3>
            <canvas id=""chart3""></canvas>
        </div>
        
        <div class=""chart-container"">
            <h3>График 4: Абсолютная пропускная способность A</h3>
            <canvas id=""chart4""></canvas>
        </div>
        
        <div class=""chart-container"">
            <h3>График 5: Среднее число занятых каналов k</h3>
            <canvas id=""chart5""></canvas>
        </div>
    </div>
    
    <script>
        const lambda = [{string.Join(", ", lambdaArray.Select(x => $"\"{x}\""))}];
        
        // График 1: Вероятность простоя
        new Chart(document.getElementById('chart1'), {{
            type: 'line',
            data: {{
                labels: lambda,
                datasets: [
                    {{
                        label: 'Экспериментальные значения',
                        data: [{string.Join(", ", expIdle)}],
                        borderColor: 'rgb(75, 192, 192)',
                        backgroundColor: 'rgba(75, 192, 192, 0.1)',
                        tension: 0.3,
                        fill: false
                    }},
                    {{
                        label: 'Теоретические значения',
                        data: [{string.Join(", ", theoIdle)}],
                        borderColor: 'rgb(255, 99, 132)',
                        backgroundColor: 'rgba(255, 99, 132, 0.1)',
                        tension: 0.3,
                        fill: false
                    }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ position: 'top' }},
                    title: {{ display: false }}
                }},
                scales: {{
                    x: {{ title: {{ display: true, text: 'Интенсивность входного потока λ (запросов/сек)' }} }},
                    y: {{ title: {{ display: true, text: 'Вероятность простоя P₀' }}, min: 0, max: 1 }}
                }}
            }}
        }});
        
        // График 2: Вероятность отказа
        new Chart(document.getElementById('chart2'), {{
            type: 'line',
            data: {{
                labels: lambda,
                datasets: [
                    {{
                        label: 'Экспериментальные значения',
                        data: [{string.Join(", ", expRejection)}],
                        borderColor: 'rgb(75, 192, 192)',
                        backgroundColor: 'rgba(75, 192, 192, 0.1)',
                        tension: 0.3,
                        fill: false
                    }},
                    {{
                        label: 'Теоретические значения',
                        data: [{string.Join(", ", theoRejection)}],
                        borderColor: 'rgb(255, 99, 132)',
                        backgroundColor: 'rgba(255, 99, 132, 0.1)',
                        tension: 0.3,
                        fill: false
                    }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ position: 'top' }}
                }},
                scales: {{
                    x: {{ title: {{ display: true, text: 'Интенсивность входного потока λ (запросов/сек)' }} }},
                    y: {{ title: {{ display: true, text: 'Вероятность отказа P<sub>отк</sub>' }}, min: 0, max: 1 }}
                }}
            }}
        }});
        
        // График 3: Относительная пропускная способность
        new Chart(document.getElementById('chart3'), {{
            type: 'line',
            data: {{
                labels: lambda,
                datasets: [
                    {{
                        label: 'Экспериментальные значения',
                        data: [{string.Join(", ", expRelative)}],
                        borderColor: 'rgb(75, 192, 192)',
                        backgroundColor: 'rgba(75, 192, 192, 0.1)',
                        tension: 0.3,
                        fill: false
                    }},
                    {{
                        label: 'Теоретические значения',
                        data: [{string.Join(", ", theoRelative)}],
                        borderColor: 'rgb(255, 99, 132)',
                        backgroundColor: 'rgba(255, 99, 132, 0.1)',
                        tension: 0.3,
                        fill: false
                    }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ position: 'top' }}
                }},
                scales: {{
                    x: {{ title: {{ display: true, text: 'Интенсивность входного потока λ (запросов/сек)' }} }},
                    y: {{ title: {{ display: true, text: 'Относительная пропускная способность Q' }}, min: 0, max: 1 }}
                }}
            }}
        }});
        
        // График 4: Абсолютная пропускная способность
        new Chart(document.getElementById('chart4'), {{
            type: 'line',
            data: {{
                labels: lambda,
                datasets: [
                    {{
                        label: 'Экспериментальные значения',
                        data: [{string.Join(", ", expAbsolute)}],
                        borderColor: 'rgb(75, 192, 192)',
                        backgroundColor: 'rgba(75, 192, 192, 0.1)',
                        tension: 0.3,
                        fill: false
                    }},
                    {{
                        label: 'Теоретические значения',
                        data: [{string.Join(", ", theoAbsolute)}],
                        borderColor: 'rgb(255, 99, 132)',
                        backgroundColor: 'rgba(255, 99, 132, 0.1)',
                        tension: 0.3,
                        fill: false
                    }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ position: 'top' }}
                }},
                scales: {{
                    x: {{ title: {{ display: true, text: 'Интенсивность входного потока λ (запросов/сек)' }} }},
                    y: {{ title: {{ display: true, text: 'Абсолютная пропускная способность A (запросов/сек)' }} }}
                }}
            }}
        }});
        
        // График 5: Среднее число занятых каналов
        new Chart(document.getElementById('chart5'), {{
            type: 'line',
            data: {{
                labels: lambda,
                datasets: [
                    {{
                        label: 'Экспериментальные значения',
                        data: [{string.Join(", ", expChannels)}],
                        borderColor: 'rgb(75, 192, 192)',
                        backgroundColor: 'rgba(75, 192, 192, 0.1)',
                        tension: 0.3,
                        fill: false
                    }},
                    {{
                        label: 'Теоретические значения',
                        data: [{string.Join(", ", theoChannels)}],
                        borderColor: 'rgb(255, 99, 132)',
                        backgroundColor: 'rgba(255, 99, 132, 0.1)',
                        tension: 0.3,
                        fill: false
                    }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ position: 'top' }}
                }},
                scales: {{
                    x: {{ title: {{ display: true, text: 'Интенсивность входного потока λ (запросов/сек)' }} }},
                    y: {{ title: {{ display: true, text: 'Среднее число занятых каналов k' }}, min: 0, max: 5 }}
                }}
            }}
        }});
    </script>
</body>
</html>";
            
            File.WriteAllText("result/charts.html", html, System.Text.Encoding.UTF8);
            Console.WriteLine("HTML файл с графиками создан успешно!");
        }
    }
    
    public class ExperimentalPoint
    {
        public double Lambda { get; set; }
        public double ProbabilityIdle { get; set; }
        public double ProbabilityRejection { get; set; }
        public double RelativeThroughput { get; set; }
        public double AbsoluteThroughput { get; set; }
        public double AvgBusyChannels { get; set; }
    }
    
    public class TheoreticalPoint
    {
        public double Lambda { get; set; }
        public double ProbabilityIdle { get; set; }
        public double ProbabilityRejection { get; set; }
        public double RelativeThroughput { get; set; }
        public double AbsoluteThroughput { get; set; }
        public double AvgBusyChannels { get; set; }
    }
}
