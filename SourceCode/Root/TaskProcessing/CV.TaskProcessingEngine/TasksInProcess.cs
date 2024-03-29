﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Data;

namespace CV.TaskProcessing
{
    /// <summary>
    /// Utility class of the Task Processing Engine (TPE) which is used as a data structure
    /// for the tasks that are currently being processed.
    /// </summary>
    internal class TasksInProcess
    {
        TaskProcess _taskProcess;
        Thread _thread;
        DequeuedTask _dequeuedTask;

        internal TasksInProcess(TaskProcess taskProcess, Thread thread, DequeuedTask dequeuedTask)
        {
            _taskProcess = taskProcess;
            _thread = thread;
            _dequeuedTask = dequeuedTask;
        }

        internal TaskProcess Process
        {
            get { return _taskProcess; }
        }

        internal Thread ProcessThread
        {
            get { return _thread; }
        }

        internal DequeuedTask DequeuedTaskData
        {
            get { return _dequeuedTask; }
        }

    }
}
