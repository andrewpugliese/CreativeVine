using System;
using System.Data;
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
using System.Threading;
using System.Diagnostics;

using CV.Configuration;
using CV.Database;
using CV.Wpf.Controls;

namespace CV.Utility.DbSetup
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window, IProcessControl
    {
        DbSetupMgr _dbSetupMgr = null;
        DbSetupElement _dbSetupConfig = null;

        public MainWindow()
        {
            InitializeComponent();
            DbSetupConfiguration dbSetupConfigSection
                     = ConfigurationMgr.GetSection<DbSetupConfiguration>(DbSetupConfiguration.ConfigSectionName);

            string dbSetupKey = ConfigurationMgr.GetNonEmptyValue(Configuration.Constants.DbSetupKey);
            _dbSetupConfig = dbSetupConfigSection.GetDbSetupConfig(dbSetupKey);

            pcbMain.SetContext(this);

            DisplayConfigSettings();
            Ready();
        }

        Database.Provider.DatabaseTypeName GetDatabaseType(string TypeName)
        {
            if (Enum.IsDefined(typeof(Database.Provider.DatabaseTypeName), TypeName))
                return (Database.Provider.DatabaseTypeName)
                    Enum.Parse(typeof(Database.Provider.DatabaseTypeName), TypeName, true);
            else throw new ArgumentOutOfRangeException(string.Format("Undefined Database Type Name: {0}", TypeName));
        }

        void DisplayConfigSettings()
        {
            DataTable configSettings = new DataTable();
            configSettings.Columns.Add(new DataColumn(Constants.ConfigKey));
            configSettings.Columns.Add(new DataColumn(Constants.ConfigValue));

            DataRow row = configSettings.NewRow();
            row[Constants.ConfigKey] = Configuration.Constants.DbSetupKey;
            row[Constants.ConfigValue] = _dbSetupConfig.DbSetupKey;
            configSettings.Rows.Add(row);

            row = configSettings.NewRow();
            row[Constants.ConfigKey] = Configuration.Constants.DbServer;
            row[Constants.ConfigValue] = _dbSetupConfig.DbServer;
            configSettings.Rows.Add(row);

            row = configSettings.NewRow();
            row[Constants.ConfigKey] = Constants.DbType;
            row[Constants.ConfigValue] = _dbSetupConfig.DbType;
            configSettings.Rows.Add(row);

            row = configSettings.NewRow();
            row[Constants.ConfigKey] = Configuration.Constants.DbName;
            row[Constants.ConfigValue] = _dbSetupConfig.DbName;
            configSettings.Rows.Add(row);

            row = configSettings.NewRow();
            row[Constants.ConfigKey] = Configuration.Constants.OutputFileName;
            row[Constants.ConfigValue] = _dbSetupConfig.OutputFileName;
            configSettings.Rows.Add(row);

            row = configSettings.NewRow();
            row[Constants.ConfigKey] = Configuration.Constants.InputFileName;
            row[Constants.ConfigValue] = _dbSetupConfig.InputFileName;
            configSettings.Rows.Add(row);

            row = configSettings.NewRow();
            row[Constants.ConfigKey] = Configuration.Constants.DDLSourceDirectory;
            row[Constants.ConfigValue] = _dbSetupConfig.DDLSourceDirectory;
            configSettings.Rows.Add(row);

            row = configSettings.NewRow();
            row[Constants.ConfigKey] = Configuration.Constants.TextEditor;
            row[Constants.ConfigValue] = _dbSetupConfig.TextEditor;
            configSettings.Rows.Add(row);

            row = configSettings.NewRow();
            row[Constants.ConfigKey] = Configuration.Constants.UserName;
            row[Constants.ConfigValue] = _dbSetupConfig.UserName;
            configSettings.Rows.Add(row);

            row = configSettings.NewRow();
            row[Constants.ConfigKey] = Configuration.Constants.UserPassword;
            row[Constants.ConfigValue] = _dbSetupConfig.UserPassword;
            configSettings.Rows.Add(row);

            dgConfigSettings.ItemsSource = configSettings.DefaultView;
        }
        
        void Ready()
        {
            tblResults.Text = string.Format("Review Config Settings; Click <Start> button to begin.{0}", Environment.NewLine);
        }

        public bool Start(object Context)
        {
            if (MessageBox.Show("Are you sure you want to recreate the database?"
                , "Database Setup"
                , MessageBoxButton.YesNo
                , MessageBoxImage.Exclamation) == MessageBoxResult.Yes)
            {
                _dbSetupMgr = new DbSetupMgr(_dbSetupConfig.DbServer, _dbSetupConfig.DbName
                    , _dbSetupConfig.UserName
                    , _dbSetupConfig.UserPassword
                    , GetDatabaseType(_dbSetupConfig.DbType)
                    , Convert.ToBoolean(_dbSetupConfig.AsSysDba)
                    , _dbSetupConfig.InputFileName
                    , _dbSetupConfig.OutputFileName
                    , _dbSetupConfig.DDLSourceDirectory
                    , _dbSetupConfig.Params
                    , DbSetupCompleted);

                Thread t = new Thread(_dbSetupMgr.Start);
                t.IsBackground = true;
                t.Start();
                return true;
            }
            else return false;
        }

        public bool Stop(object Context)
        {
            if (_dbSetupMgr != null)
                _dbSetupMgr.Stop();
            return true;
        }

        public void Pause(object Context)
        {
            if (_dbSetupMgr != null)
                _dbSetupMgr.Pause();
        }

        public void Resume(object Context)
        {
            if (_dbSetupMgr != null)
                _dbSetupMgr.Resume();
        }

        public string Status(object Context)
        {
            tblResults.Text = Context != null ? Context.ToString() + _dbSetupMgr.Status : _dbSetupMgr.Status;
            return tblResults.Text;
        }

        public void DisplayPausedState()
        {
        }

        public void DisplayResumedState()
        {
        }

        /// <summary>
        /// Called by worker thread when completed parsing command file (or was interrupted).
        /// </summary>
        internal void DbSetupCompleted(String results, bool aborted, TimeSpan timespan)
        {
            string msg = string.Format("DbSetup {0}; time elapsed: seconds: {1}, milliseconds: {2}{3}."
                , aborted ? "Aborted" : "Completed"
                , timespan.TotalSeconds
                , timespan.TotalMilliseconds
                , Environment.NewLine);
            MessageBox.Show(msg);
            Dispatcher.BeginInvoke(System.Windows.Threading.DispatcherPriority.Normal, (Action)(() => { Status(msg); }));
            pcbMain.DisplayStoppedState();
        }

        private void gridDbSetup_Loaded(object sender, RoutedEventArgs e)
        {
            int numCols = dgConfigSettings.Columns.Count;
            for (int i = 0; i < numCols; i++)
            {
                DataGridColumn dgc = dgConfigSettings.Columns[i];
                if (i == (numCols - 1))
                    dgc.Width = new DataGridLength(1.0, DataGridLengthUnitType.Star);
                else dgc.Width = new DataGridLength(1.0, DataGridLengthUnitType.SizeToCells);
            }
            dgConfigSettings.HorizontalScrollBarVisibility = ScrollBarVisibility.Auto;
        }

        private void pcbMain_Loaded(object sender, RoutedEventArgs e)
        {

        }

        private void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            Stop(null);
            Application.Current.Shutdown();
        }

        private void dgConfigSettings_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            var grid = sender as DataGrid;

            var cellValue = grid.SelectedValue;
            DataRow dr = ((System.Data.DataRowView)(grid.SelectedItem)).Row;
            if (dr[Constants.ConfigKey].ToString() == Configuration.Constants.OutputFileName)
                ViewFile(_dbSetupConfig.DDLSourceDirectory + "\\" + _dbSetupConfig.OutputFileName);
            else if (dr[Constants.ConfigKey].ToString() == Configuration.Constants.InputFileName)
                ViewFile(_dbSetupConfig.DDLSourceDirectory + "\\" + _dbSetupConfig.InputFileName);
        }

        private void ViewFile(string FileName)
        {
            ProcessStartInfo psi = new ProcessStartInfo();
            psi.FileName = _dbSetupConfig.TextEditor;
            Process p = new Process();
            psi.Arguments = FileName;

            p.StartInfo = psi;
            p.Start();
        }
    }
}
