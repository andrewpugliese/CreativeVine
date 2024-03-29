﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace CV.Wpf.Controls
{
    /// <summary>
    /// Interaction logic for ProcessControlBar.xaml
    /// The Process Control Bar is a user control that contains buttons for Start,
    /// Stop, Pause, Resume, Status that can be used to control any process.
    /// </summary>
    public partial class ProcessControlBar : UserControl
    {
        IProcessControl _parentControl = null;
        object _parentContext = null;
        enum ProcessControlState { Stopped, Started, Paused, Resumed };

        /// <summary>
        /// Default constructor
        /// </summary>
        public ProcessControlBar()
        {
            InitializeComponent();
            DiplayStoppedState();
        }

        /// <summary>
        /// Sets/Resets the status of the proces control bar and its controls to enabled or dissabled
        /// </summary>
        public bool Enabled
        {
            get
            {
                return this.IsEnabled;
            }
            set
            {
                if (this.IsEnabled != value)
                    this.IsEnabled = value;
            }
        }

        /// <summary>
        /// Dispatches a request to set / reset buttons so display a paused state.
        /// </summary>
        public void DisplayPausedState()
        {
            Dispatcher.Invoke(System.Windows.Threading.DispatcherPriority.Send
                                 , new Action<ProcessControlState>(UpdateButtons),
                                 ProcessControlState.Paused);
        }

        /// <summary>
        /// Dispatches a request to set / reset buttons so display a resumed or running state.
        /// </summary>
        public void DisplayResumedState()
        {
            Dispatcher.Invoke(System.Windows.Threading.DispatcherPriority.Send
                                 , new Action<ProcessControlState>(UpdateButtons),
                                 ProcessControlState.Resumed);
        }

        /// <summary>
        /// Dispatches a request to set / reset buttons so display a disconnected state.
        /// </summary>
        public void DisplayStoppedState()
        {
            Dispatcher.Invoke(System.Windows.Threading.DispatcherPriority.Send
                                 , new Action<ProcessControlState>(UpdateButtons),
                                 ProcessControlState.Stopped);
        }

        /// <summary>
        /// Changes state of buttons to display the given process control state
        /// </summary>
        /// <param name="processControlState">State of the process to display</param>
        void UpdateButtons(ProcessControlState processControlState)
        {
            if (processControlState == ProcessControlState.Paused)
            {
                btnResume.IsEnabled = true;
                btnPause.IsEnabled = !btnResume.IsEnabled;
            }
            else if (processControlState == ProcessControlState.Resumed)
            {
                btnResume.IsEnabled = false;
                btnPause.IsEnabled = !btnResume.IsEnabled;
            }
            else if (processControlState == ProcessControlState.Stopped)
            {
                DiplayStoppedState();
            }
        }

        /// <summary>
        /// Sets the context for which this control will be embedded
        /// </summary>
        /// <param name="parentControl">The interface instance from the parent</param>
        /// <param name="parentContext">The context provided by the parent</param>
        /// <param name="btnStartContent">Button text override string</param>
        /// <param name="btnStopContent">Button text override string</param>
        /// <param name="btnPauseContent">Button text override string</param>
        /// <param name="btnResumeContent">Button text override string</param>
        /// <param name="btnStatusContent">Button text override string</param>
        public void SetContext(IProcessControl parentControl
                , object parentContext = null
                , string btnStartContent = null
                , string btnStopContent = null
                , string btnPauseContent = null
                , string btnResumeContent = null
                , string btnStatusContent = null)
        {
            _parentControl = parentControl;
            _parentContext = parentContext;
            if (!string.IsNullOrEmpty(btnStartContent))
                btnStart.Content = btnStartContent;
            if (!string.IsNullOrEmpty(btnStopContent))
                btnStop.Content = btnStopContent;
            if (!string.IsNullOrEmpty(btnPauseContent))
                btnPause.Content = btnPauseContent;
            if (!string.IsNullOrEmpty(btnResumeContent))
                btnResume.Content = btnResumeContent;
            if (!string.IsNullOrEmpty(btnStatusContent))
                btnStatus.Content = btnStatusContent;
        }


        void DiplayStoppedState()
        {
            btnStart.IsEnabled = true;
            btnPause.IsEnabled = btnStop.IsEnabled = btnStatus.IsEnabled = btnResume.IsEnabled = !btnStart.IsEnabled;
        }

        private void btnStart_Click(object sender, RoutedEventArgs e)
        {
            if (_parentControl.Start(_parentContext))
            {
                btnStart.IsEnabled = btnResume.IsEnabled = false;
                btnPause.IsEnabled = btnStop.IsEnabled = btnStatus.IsEnabled = !btnStart.IsEnabled;
            }
        }

        private void btnStop_Click(object sender, RoutedEventArgs e)
        {
            DiplayStoppedState();
            _parentControl.Stop(_parentContext);
        }

        private void btnPause_Click(object sender, RoutedEventArgs e)
        {
            DisplayPausedState();
            _parentControl.Pause(_parentContext);
        }

        private void btnResume_Click(object sender, RoutedEventArgs e)
        {
            DisplayResumedState();
            _parentControl.Resume(_parentContext);
        }

        private void btnStatus_Click(object sender, RoutedEventArgs e)
        {
            _parentControl.Status(_parentContext);
        }

    }
}
