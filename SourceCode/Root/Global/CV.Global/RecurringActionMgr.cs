using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CV.Global
{
    /// <summary>
    /// This class stores the action item which needs to be called periodically.
    /// </summary>
    public class ActionItem<T>
    {
        T _identifier;
        Action<T> _funcToCall;
        int _timerCycleCount;

        int _cycleCount = 0;

        public ActionItem(T identifier
            , Action<T> funcToCall
            , int timerCycleCount)
        {
            _identifier = identifier;
            _funcToCall = funcToCall;
            _timerCycleCount = timerCycleCount;
        }

        public void TimeElapsed()
        {
            _cycleCount ++;
            if (_cycleCount == _timerCycleCount)
            {
                _cycleCount = 0;

                // Call function in a default threadpool thread to avoid long running function to hold the timer
                System.Threading.ThreadPool.QueueUserWorkItem(state => _funcToCall(_identifier));
            }
        }
    }

    /// <summary>
    /// This class manages multiple callback functions which need to be called periodically. This class lazily
    /// instantiates one server timer which sends out tick every 10 seconds. Each ActionItem computes
    /// how many ticks are required before they need to call the registered function and calls them when that many ticks
    /// are received by them. This allows multiple periodic function with one single timer. Cache manager can use this
    /// function to update its cache values. Configuration manager can refresh the configuration values from database
    /// at certain intervals. The timings are in the increment of 10 seconds and will not be exact.
    /// 
    ///     void func1(string identifier)
    ///     {
    ///     }
    /// 
    ///     // Invoke "func1" function every 100 seconds.
    ///     RecurringActionMgr.Add("MyTimer1", func1, 100);
    /// 
    ///     // Invoke "func2" function every 50 seconds.
    ///     RecurringActionMgr.Add("MyTimer2", func2, 50);
    /// 
    /// </summary>
    public static class RecurringActionMgr<T>
    {
        static int _timerSecs = 10;
        static System.Timers.Timer _serverTimer = null;
        static Dictionary<T, ActionItem<T>> _actionItems = new Dictionary<T, ActionItem<T>>();
        static object _serverTimerLock = new object();

        /// <summary>
        /// Add a function which will be called at the specified interval seconds. 
        /// </summary>
        public static void Add(T identifier
            , Action<T> funcToCall
            , Action<Exception> funcOnException
            , int intervalSecs)
        {
            // Calculate how many timer cycle will need to call the function
            int remainder;
            int cycleFrequency = Math.DivRem(intervalSecs, _timerSecs, out remainder);
            if (remainder > 0 || cycleFrequency == 0) ++cycleFrequency;

            // Wrap the function to call in try catch block so that unhandled exception in user function does not
            // trash the timer.
            Action<T> tryCatchWrapper = id =>
            {
                try { funcToCall(id); }
                catch (Exception ex) { if (funcOnException != null) funcOnException(ex); }
            };

            lock (_actionItems)
            {
                _actionItems.Add(identifier
                    , new ActionItem<T>(identifier, tryCatchWrapper, cycleFrequency));
            }

            // Start the timer if it is NOT already started
            if (_serverTimer == null)
            {
                lock (_serverTimerLock)
                {
                    if (_serverTimer == null)
                    {
                        _serverTimer = new System.Timers.Timer(_timerSecs * 1000);
                        _serverTimer.Elapsed += new System.Timers.ElapsedEventHandler(_serverTimer_Elapsed);
                        _serverTimer.Start();
                    }
                }
            }
        }

        /// <summary>
        /// Remove the recurring callback function. 
        /// </summary>
        public static void Remove(T identifier)
        {
            lock (_actionItems)
            {
                _actionItems.Remove(identifier);
                if (_actionItems.Count == 0)
                    if (_serverTimer != null)
                    {
                        _serverTimer.Stop();
                        _serverTimer.Dispose();
                        _serverTimer = null;
                    }
            }
        }

        /// <summary>
        /// Returns boolean indicating whether the given identifier exists in the callback set
        /// </summary>
        /// <param name="identifier">User defined string token to associate with callback</param>
        /// <returns>true or false whether identifier exists</returns>
        static public bool ContainsKey(T identifier)
        {
            lock (_actionItems)
            {
                return _actionItems.ContainsKey(identifier);
            }
        }

        /// <summary>
        /// One server timer dispatches the time elapsed message to every registered callback items.
        /// </summary>
        static void _serverTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            // Send TimeElapsed to all the items
            lock (_actionItems)
            {
                _actionItems.Values.ToList().ForEach(item => item.TimeElapsed());
            }
        }
    }
}
