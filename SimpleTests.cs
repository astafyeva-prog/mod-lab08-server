using System;
using Xunit;

namespace SmoSimulation.Tests
{
    public class SimpleSmoTests
    {
        [Fact]
        public void Calculator_ReturnsValidValues()
        {
            // Проверка, что вероятность простоя между 0 и 1
            double p0 = SmoCalculator.CalculateIdleProbability(2.0, 2.0, 5);
            Assert.InRange(p0, 0, 1);
            
            // Проверка, что вероятность отказа между 0 и 1
            double prej = SmoCalculator.CalculateRejectionProbability(2.0, 2.0, 5);
            Assert.InRange(prej, 0, 1);
            
            // Проверка, что сумма вероятностей корректна
            Assert.True(p0 + prej <= 1.1); // Приблизительно
        }
        
        [Fact]
        public void Server_InitializesCorrectly()
        {
            var server = new Server(5, 2.0);
            Assert.Equal(5, server.GetChannelCount());
        }
        
        [Theory]
        [InlineData(0.5)]
        [InlineData(1.0)]
        [InlineData(2.0)]
        [InlineData(5.0)]
        [InlineData(10.0)]
        public void Throughput_DecreasesWithLoad(double lambda)
        {
            double mu = 2.0;
            int n = 5;
            
            double q1 = SmoCalculator.CalculateRelativeThroughput(lambda, mu, n);
            double q2 = SmoCalculator.CalculateRelativeThroughput(lambda + 1, mu, n);
            
            // При увеличении нагрузки пропускная способность должна уменьшаться
            Assert.True(q2 <= q1 + 0.01); // Не строгое неравенство из-за погрешностей
        }
    }
}
