using System;
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
using System.Windows.Threading;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Data;

using CV.Database;
using CV.Database.Sequence;
using CV.Configuration;
using CV.Logging;

namespace CV.Utility.Testing
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        bool _insertStarted = false;
        public MainWindow()
        {
            InitializeComponent();
            DatabaseMgr dbMgr = new DatabaseMgr(ConfigurationMgr.GetNonEmptyValue("ConnectionKey"));
            // create paging manager for task processing queue
            PagingMgr testData = new PagingMgr(dbMgr
                    , string.Format("{0}.{1}", Database.Constants.SCHEMA_CORE, Constants.TABLE_TestData)
                    , null, 100, null);

            // pass paging manager to paging controll
            ptTestData.Source = testData;
            ptTestData.Title = "Test Data";
        }

        private void pcbMain_Loaded(object sender, RoutedEventArgs e)
        {

        }

        public void Stop()
        {
            if (_insertStarted)
                DbCommandMgrTest.Stop();
        }

        private void DisplayConnectionData(DatabaseMgr dbMgr)
        {

        }
        private void ptTestData_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            //var grid = sender as DataGrid;

           // var cellValue = grid.SelectedValue;
           // DataRow dr = ((System.Data.DataRowView)(grid.SelectedItem)).Row;
        }

        /*
        private void gridTestDbMgr_Loaded(object sender, RoutedEventArgs e)
        {
            int numCols = dgTestData.Columns.Count;
            for (int i = 0; i < numCols; i++)
            {
                DataGridColumn dgc = dgTestData.Columns[i];
                if (i == (numCols - 1))
                    dgc.Width = new DataGridLength(1.0, DataGridLengthUnitType.Star);
                else dgc.Width = new DataGridLength(1.0, DataGridLengthUnitType.SizeToCells);
            }
            dgTestData.HorizontalScrollBarVisibility = ScrollBarVisibility.Auto;
        }
        */
        private void btnConnect_Click(object sender, RoutedEventArgs e)
        {
            TestSequence();
        }

        void TestSequence()
        {
            DatabaseMgr dbMgr = new DatabaseMgr(ConfigurationMgr.GetNonEmptyValue("ConnectionKey"));


            using (SequenceMgr seqMgr = new SequenceMgr(dbMgr))
            {
                seqMgr.SetupKey("MyTestKey", 5, 1);
                seqMgr.SetupKey("MyTestKey", 5, 1);
                long sequence = seqMgr.GetNextSequence("MyTestKey");
                sequence = seqMgr.GetNextSequence("MyTestKey");
                sequence = seqMgr.GetNextSequence("MyTestKey");
                sequence = seqMgr.GetNextSequence("MyTestKey");
                sequence = seqMgr.GetNextSequence("MyTestKey");
                sequence = seqMgr.GetNextSequence("MyTestKey");
                sequence = seqMgr.GetNextSequence("MyTestKey");
                sequence = seqMgr.GetNextSequence("MyTestKey");
                sequence = seqMgr.GetNextSequence("MyTestKey");
                sequence = seqMgr.GetNextSequence("MyTestKey");
                sequence = seqMgr.GetNextSequence("MyTestKey");
            }
            using (SequenceMgr seqMgr = new SequenceMgr(dbMgr, 10))
            {
                seqMgr.SetupKey("MyTestKey2", 50, 1);
                seqMgr.SetupKey("MyTestKey2", 50, 1);
                long sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
                sequence = seqMgr.GetNextSequence("MyTestKey2");
            }
        }

        private void btnThreading_Click(object sender, RoutedEventArgs e)
        {

        }

        private void btnMemFile_Click(object sender, RoutedEventArgs e)
        {
        }

        private void btnLogging_Click(object sender, RoutedEventArgs e)
        {
        }

        private void btnInsert_Click(object sender, RoutedEventArgs e)
        {
            _insertStarted = !_insertStarted;
            btnInsert.Content = _insertStarted ? "StopInsert" : "StartInsert";
            DatabaseMgr dbMgr = new DatabaseMgr(ConfigurationMgr.GetNonEmptyValue("ConnectionKey"));
            Logging.LoggingMgr lm = new LoggingMgr("BasicLogging");
            DbCommandMgrTest insertTest = new DbCommandMgrTest(dbMgr, lm);
            insertTest.Insert(_insertStarted);
        }

        private void wdwMain_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            Stop();
            Application.Current.Shutdown();
        }

    }
}
