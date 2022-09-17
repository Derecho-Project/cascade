using CommonGatewayWorkerLib;
using LoggerLib;

namespace WorkerLib
{
    public class Worker : IWorker
    {
        private const int TIMER_PERIOD_IN_MS = 3000;

        private Logger _logger = new Logger();
        private Timer _timer;

        // Implementing IWorker
        public Dictionary<string, Func<string, string, UnmanagedCallbackDelegate, string>> Functions =>
                new Dictionary<string, Func<string, string, UnmanagedCallbackDelegate, string>>
                {
                    { "GetDevice", GetDevice },
                    { "SubscribeForDevice", SubscribeForDevice },
                    { "UnsubscribeFromDevice", UnsubscribeFromDevice }
                };

        #region Methods called from unmanaged code with mediation of gateway

        public string GetDevice(string funcName, string jsonArgs, UnmanagedCallbackDelegate callback)
        {
            _logger.Info($"    GetDevice(funcName: {funcName}, jsonArgs: {jsonArgs})");

            var device = new Device
            {
                Type = "TypeX",
                Id = 1001,
                IsValid = true,
                Properties = new List<string>
                {
                    $"{DateTime.Now.Ticks}",
                    "prop2",
                    "prop3"
                },
                Value = 0
            };

            _logger.Info($"    unmanaged_callback(funcName: {funcName}, jsonArgs: {device.AsJsonString})");

            bool? bRet = false;
            try
            {
                bRet = callback?.Invoke(funcName, device.AsJsonString);
            }
            catch (Exception e)
            {
                _logger.Error($"ERROR while executing callback. {Environment.NewLine}{e}");
            }

            if (bRet != true)
                _logger.Error("ERROR while executing callback in method \"GetDevice\"");

            return $"Response from managed code: Method \"{funcName}\" was invoked and returned \"{bRet}\"";
        }

        public string SubscribeForDevice(string funcName, string jsonArgs, UnmanagedCallbackDelegate callback)
        {
            if (_timer != null)
                return "Client has already subscribed for Device";

            _logger.Info($"    SubscribeForDevice(funcName: {funcName}, jsonArgs: {jsonArgs})");

            string[] colors = {"white", "blue", "red", "yellow", "green"};

            if (callback == null)
                return "Client failed to subscribe for Device: no callback provided";

            var random = new Random(5);

            _timer = new Timer(_ =>
            {
                var device = new Device
                {
                    Type = "TypeX",
                    Id = 1001,
                    IsValid = true,
                    Properties = new List<string>
                    {
                        colors[random.Next(0, 4)],
                        colors[random.Next(0, 4)],
                        colors[random.Next(0, 4)],
                    },
                    Value = random.Next(0, 99)
                };

                _logger.Info($"    unmanaged_callback(funcName: {funcName}, jsonArgs: {device.AsJsonString})");

                try
                {
                    if (!callback(funcName, device.AsJsonString))
                        new Exception("ERROR: wrong result on callback call.");
                }
                catch (Exception e)
                {
                    _logger.Error($"ERROR while executing callback. {Environment.NewLine}{e}");
                }
            }, null, TIMER_PERIOD_IN_MS, TIMER_PERIOD_IN_MS);

            return "Client has successfully subscribed for Device";
        }

        public string UnsubscribeFromDevice(string funcName, string jsonArgs, UnmanagedCallbackDelegate callback)
        {
            if (_timer == null)
                return "Client has not yet unsubscribed for Device";

            _timer.Dispose();
            _timer = null;

            return "Client has successfully unsubscribed from Device";
        }

        #endregion // Methods called from unmanaged code with mediation of gateway
    }
}
